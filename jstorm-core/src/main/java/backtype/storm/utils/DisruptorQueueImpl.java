/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.utils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.daemon.worker.Flusher;
import com.alibaba.jstorm.utils.JStormUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.disruptor.AbstractSequencerExt;
import backtype.storm.utils.disruptor.RingBuffer;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
//import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 *
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is the ability to catch up to the producer by processing tuples in batches.
 */
public class DisruptorQueueImpl extends DisruptorQueue {
    private static final Logger LOG = LoggerFactory.getLogger(DisruptorQueueImpl.class);
    static boolean useSleep = true;

    public static void setUseSleep(boolean useSleep) {
        AbstractSequencerExt.setWaitSleep(useSleep);
    }

    private static final Object INTERRUPT = new Object();
    private static final String PREFIX = "disruptor-";

    private final String _queueName;
    private final RingBuffer<MutableObject> _buffer;
    private final Sequence _consumer;
    private final SequenceBarrier _barrier;

    private boolean _isBatch;
    private ThreadLocalBatch _batcher;
    private int _inputBatchSize;
    private Flusher _flusher;

    private Object _callbackLock = new Object();
    private List<Callback> _callbacks = null;
    private Object _cacheLock = new Object();
    private List<Object> _cache;

    // TODO: consider having a threadlocal cache of this variable to speed up
    // reads?
    //volatile boolean consumerStartedFlag = false;
    private final HashMap<String, Object> state = new HashMap<String, Object>(4);

    public DisruptorQueueImpl(String queueName, ProducerType producerType, int bufferSize, WaitStrategy wait, boolean isBatch,
                              int batchSize, long flushMs) {
        _queueName = PREFIX + queueName;
        _buffer = RingBuffer.create(producerType, new ObjectEventFactory(), bufferSize, wait);
        _consumer = new Sequence();
        _barrier = _buffer.newBarrier();
        _buffer.addGatingSequences(_consumer);
        _isBatch = isBatch;
        _cache = new ArrayList<Object>();
        if (_isBatch) {
            _inputBatchSize = batchSize;
            _batcher = new ThreadLocalBatch();
            _flusher = new DisruptorFlusher(Math.max(flushMs, 1));
            _flusher.start();
        } else {
            _batcher = null;
        }
    }

    public String getName() {
        return _queueName;
    }

    public void consumeBatch(EventHandler<Object> handler) {
        //write pos > read pos
        //Asynchronous release the queue, but still is single thread
        if (_buffer.getCursor() > _consumer.get())
            consumeBatchWhenAvailable(handler);
    }

    public void haltWithInterrupt() {
        publish(INTERRUPT);
    }

    public Object poll() {
    	Object ret = null;
    	if (cacheSize() > 0) { 
    		synchronized (_cacheLock) {
    			ret = _cache.remove(0);
    		}
    	} else {
            final long nextSequence = _consumer.get() + 1;
            if (nextSequence <= _barrier.getCursor()) {
                MutableObject mo = _buffer.get(nextSequence);
                _consumer.set(nextSequence);
                ret = mo.o;
                mo.setObject(null);
            }
    	}
    	handlerCallback();
        return ret;
    }

    public Object take() {
    	Object ret = null;
    	if (cacheSize() > 0) { 
    		synchronized (_cacheLock) {
    			ret = _cache.remove(0);
    		}
    	} else {
            final long nextSequence = _consumer.get() + 1;
            // final long availableSequence;
            try {
                _barrier.waitFor(nextSequence);
            } catch (AlertException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException " + e.getCause());
                // throw new RuntimeException(e);
                return null;
            } catch (TimeoutException e) {
                // LOG.error(e.getCause(), e);
                return null;
            }
            MutableObject mo = _buffer.get(nextSequence);
            _consumer.set(nextSequence);
            ret = mo.o;
            mo.setObject(null);
    	}
    	handlerCallback();
        return ret;
    }

    private void handlerCallback() {
        if (_callbacks != null) {
            synchronized (_callbackLock) {
                if (_callbacks != null) {
                    Iterator<Callback> itr = _callbacks.iterator();
                    while (itr.hasNext()) {
                        Callback cb = itr.next();
                        if((boolean) cb.execute()) {
                            itr.remove();
                        }
                    }
                    if (_callbacks.size() == 0) {
                        _callbacks = null;
                    }
                }
            }
        }
    }

    public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
        consumeBatchWhenAvailable(handler, true);
    }
                
    public void consumeBatchWhenAvailableWithCallback(EventHandler<Object> handler) {
        consumeBatchWhenAvailable(handler);
        handlerCallback();
    }

    public void multiConsumeBatchWhenAvailable(EventHandler<Object> handler) {
        consumeBatchWhenAvailable(handler, false);
    }

    public void multiConsumeBatchWhenAvailableWithCallback(EventHandler<Object> handler) {
        consumeBatchWhenAvailable(handler, false);
        handlerCallback();
    }

    public void consumeBatchWhenAvailable(EventHandler<Object> handler, boolean isSync) {
    	List<Object> cache = null;
    	synchronized (_cacheLock) {
    		if (_cache.size() > 0) {
    			cache = _cache;
        	    _cache = new ArrayList<Object>();
    		}
    	}

    	if (cache != null) {
    		for (int i = 0; i < cache.size(); i++) {
                try {
                    handler.onEvent(cache.get(i), 0, i == (cache.size() - 1));
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
        } else {
            try {
                if (isSync) {
                    final long nextSequence = _consumer.get() + 1;
                    final long availableSequence = _barrier.waitFor(nextSequence);
                    if (availableSequence >= nextSequence) {
                        consumeBatchToCursor(availableSequence, handler);
                    }
                } else {
                    List<Object> batch = retreiveAvailableBatch();
                    if (batch.size() > 0) {
                        for (int i = 0; i < batch.size(); i++) {
                            try {
                                handler.onEvent(batch.get(i), 0, i == (batch.size() - 1));
                            } catch (Exception e) {
                                LOG.error(e.getMessage(), e);
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        JStormUtils.sleepMs(1);
                    }
                }
            } catch (AlertException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException " + e.getCause());
                return;
            } catch (TimeoutException e) {
                // Do nothing
            }
        }
    }

    public void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
        for (long curr = _consumer.get() + 1; curr <= cursor; curr++) {
            try {
                MutableObject mo = _buffer.get(curr);
                Object o = mo.o;
                mo.setObject(null);
                handler.onEvent(o, curr, curr == cursor);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
                return;
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        // TODO: only set this if the consumer cursor has changed?
        _consumer.set(cursor);
    }

    synchronized public List<Object> retreiveAvailableBatch() throws AlertException, InterruptedException, TimeoutException {
        final long nextSequence = _consumer.get() + 1;
        final long availableSequence = _barrier.waitFor(nextSequence);
        final Long availableNum = availableSequence - nextSequence + 1;
        List<Object> ret = new ArrayList<Object>(availableNum.intValue());
        if (availableSequence >= nextSequence) {
            for (long curr = _consumer.get() + 1; curr <= availableSequence; curr++) {
                try {
                    MutableObject mo = _buffer.get(curr);
                    Object o = mo.o;
                    mo.setObject(null);
                    ret.add(o);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            _consumer.set(availableSequence);
        }

        return ret;
    }

    /*
     * Caches until consumerStarted is called, upon which the cache is flushed to the consumer
     */
    public void publish(Object obj) {
        try {
            if (_isBatch) {
                publishBatch(obj);
            } else {
                publish(obj, true);
            }
        } catch (InsufficientCapacityException ex) {
            throw new RuntimeException("This code should be unreachable!");
        }
    }

    public void publishBatch(Object obj) {
        _batcher.addAndFlush(obj);
    }

    public void publishDirect(List<Object> batch, boolean block) throws InsufficientCapacityException {
        int size = 0;
        if (batch != null){
            size = batch.size();
            if (block) {
            	int left = size;
            	int batchIndex = 0;
            	while (left > 0) {
           	        int availableCapacity = (int) _buffer.remainingCapacity();
           	        if (availableCapacity > 0) {
           	            int n = availableCapacity >= left ? left : availableCapacity;
           	            final long end = _buffer.next(n);
           	            publishBuffer(batch, batchIndex, end - n + 1, end);

           	            left -= n;
           	            batchIndex += n; 
           	        } else {
           	        	JStormUtils.sleepMs(1);
           	        }
            	}
            } else {
                final long end = _buffer.tryNext(size);
       	        publishBuffer(batch, 0, end - size + 1, end);
       	    }
        }
    }

    private void publishBuffer(List<Object> batch, int batchIndex, long bufferBegin, long bufferEnd) {
        long at = bufferBegin;
        while (at <= bufferEnd) {
            final MutableObject m = _buffer.get(at);
            m.setObject(batch.get(batchIndex));
            at++;
            batchIndex++;
        }
        _buffer.publish(bufferBegin, bufferEnd);
    }

    public void tryPublish(Object obj) throws InsufficientCapacityException {
        publish(obj, false);
    }

    public void publish(Object obj, boolean block) throws InsufficientCapacityException {
        publishDirect(obj, block);
    }

    protected void publishDirect(Object obj, boolean block) throws InsufficientCapacityException {
        final long id;
        if (block) {
            id = _buffer.next();
        } else {
            id = _buffer.tryNext(1);
        }
        final MutableObject m = _buffer.get(id);
        m.setObject(obj);
        _buffer.publish(id);
    }

    public void publishCache(Object obj) {
    	synchronized (_cacheLock) {
    		_cache.add(obj);
    	}
    }

    public int cacheSize() {
    	synchronized (_cacheLock) {
    		return _cache.size();
    	}
    }

    public void clear() {
        while (population() != 0L) {
            poll();
        }
    }

    public long population() {
        return (writePos() - readPos());
    }

    public long capacity() {
        return _buffer.getBufferSize();
    }

    public long writePos() {
        return _buffer.getCursor();
    }

    public long readPos() {
        return _consumer.get();
    }

    public float pctFull() {
        return (1.0F * population() / capacity());
    }

    @Override
    public Object getState() {
        // get readPos then writePos so it's never an under-estimate
        long rp = readPos();
        long wp = writePos();
        state.put("capacity", capacity());
        state.put("population", wp - rp);
        state.put("write_pos", wp);
        state.put("read_pos", rp);
        return state;
    }

    public RingBuffer<MutableObject> get_buffer() {
        return _buffer;
    }

    public SequenceBarrier get_barrier() {
        return _barrier;
    }

    public void publishCallback(Callback cb) {
        if (cb != null) {
            synchronized (_callbackLock) {
                if (_callbacks == null) {
                    _callbacks = new ArrayList<Callback>();
                }
                _callbacks.add(cb);
            }
        }
    }

    public static class ObjectEventFactory implements EventFactory<MutableObject> {
        @Override
        public MutableObject newInstance() {
            return new MutableObject();
        }
    }

    private class ThreadLocalBatch {

        private ArrayList<Object> _batcher;

        public ThreadLocalBatch() {
            _batcher = new ArrayList<Object>(_inputBatchSize);
        }

        public synchronized void addAndFlush(Object obj) {
            ArrayList<Object> batchTobeFlushed = add(obj);
            if (batchTobeFlushed != null) {
                try {
                    publishDirect(batchTobeFlushed, true);
                } catch (InsufficientCapacityException e) {
                    LOG.warn("Failed to publish batch");
                }
            }
        }

        public ArrayList<Object> add(Object obj) {
            ArrayList<Object> ret = null;
            _batcher.add(obj);
            if (_batcher.size() >= _inputBatchSize) {
                ret = _batcher;
                _batcher = new ArrayList<Object>(_inputBatchSize);
            }
            return ret;
        }

        //May be called by a background thread
        public synchronized void flush() {
            List<Object> cache = null;
            try {
            	if (_batcher != null && _batcher.size() > 0) {
                    publishDirect(_batcher, true);
                    _batcher = new ArrayList<Object>(_inputBatchSize);
                }
            } catch (InsufficientCapacityException e) {
                //Ignored we should not block
            }
        }
    }

    private class DisruptorFlusher extends Flusher {
        private AtomicBoolean _isFlushing = new AtomicBoolean(false);

        public DisruptorFlusher(long flushInterval) {
            _flushIntervalMs = flushInterval;
        }

        public void run() {
            if (_isFlushing.compareAndSet(false, true)) {
                _batcher.flush();
                _isFlushing.set(false);
            }
        }
    }
}
