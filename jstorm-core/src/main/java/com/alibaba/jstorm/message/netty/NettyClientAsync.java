/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.message.netty;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.Flusher;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.messaging.NettyMessage;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

class NettyClientAsync extends NettyClient {
    private static final Logger LOG = LoggerFactory.getLogger(NettyClientAsync.class);
    public static final String PREFIX = "Netty-Client-";
    protected int flushCheckInterval;
    
    private HashMap<Integer, Condition> targetTasksUnderFlowCtrl;
    private Map<Integer, MessageBatch> targetTasksCache;
    private ConcurrentHashMap<String, Set<Integer>> remoteAddrToTasks;
    private ReentrantLock lock;
    private ReentrantLock flowCtrlLock;
    private int flowCtrlAwaitTime;
    private int cacheSize;

    private class NettyClientFlush extends Flusher {
        private AtomicBoolean _isFlushing = new AtomicBoolean(false);

        public NettyClientFlush(long flushInterval) {
            _flushIntervalMs = flushInterval;
        }

        public void run() {
            if (_isFlushing.compareAndSet(false, true)) {
                synchronized (writeLock) {
                    Channel channel = channelRef.get();
                    if (channel != null && channel.isWritable() && messageBuffer.size() > 0) {
                        MessageBatch messageBatch = messageBuffer.drain();
                        flushRequest(channel, messageBatch);
                    }
                }
                _isFlushing.set(false);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    NettyClientAsync(Map storm_conf, ChannelFactory factory, String host, int port, ReconnectRunnable reconnector) {
        super(storm_conf, factory, host, port, reconnector);

        clientChannelFactory = factory;
        targetTasksUnderFlowCtrl = new HashMap<Integer, Condition>();
        targetTasksCache = new HashMap<Integer, MessageBatch>();
        remoteAddrToTasks = new ConcurrentHashMap<String, Set<Integer>>();
        lock = new ReentrantLock();
        flowCtrlLock = new ReentrantLock();
        flowCtrlAwaitTime = ConfigExtension.getNettyFlowCtrlWaitTime(storm_conf);
        cacheSize = ConfigExtension.getNettyFlowCtrlCacheSize(storm_conf) != null ? 
        		ConfigExtension.getNettyFlowCtrlCacheSize(storm_conf) : messageBatchSize;

        flushCheckInterval = Utils.getInt(storm_conf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 5);
        Flusher flusher = new NettyClientFlush(flushCheckInterval);
        flusher.start();

        start();
        //LOG.info(this.toString());
    }

    /**
     * TODO: this interface is not compatible with the latest backpressure solution.
     *       maybe we should remove it.
     * Enqueue a task message to be sent to server
     */
    @Override
    public void send(List<TaskMessage> messages) {
        // throw exception if the client is being closed
        if (isClosed()) {
            LOG.warn("Client is being closed, and does not take requests any more");
            return;
        }

        long start = enableNettyMetrics && sendTimer != null ? sendTimer.getTime() : 0L;
        try {
            for (TaskMessage message : messages) {
            	waitforFlowCtrlAndSend(message);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sendTimer != null && enableNettyMetrics) {
                sendTimer.updateTime(start);
            }
        }
    }

    @Override
    public void send(TaskMessage message) {
        // throw exception if the client is being closed
        if (isClosed()) {
            LOG.warn("Client is being closed, and does not take requests any more");
            return;
        } else {
        	long start = enableNettyMetrics && sendTimer != null ? sendTimer.getTime() : 0L;
            try {
        	    waitforFlowCtrlAndSend(message);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (sendTimer != null && enableNettyMetrics) {
                    sendTimer.updateTime(start);
                }
            }
        }
    }

    void pushBatch(NettyMessage message) {
        if (message == null || message.isEmpty()) {
            return;
        }

        synchronized (writeLock) {
            Channel channel = channelRef.get();
            if (channel == null) {
                messageBuffer.add(message, false);
                LOG.debug("Pending requested message, the size is {}, because channel is not ready.", messageBuffer.size());
            } else {
                if (channel.isWritable()) {
                    MessageBatch messageBatch = messageBuffer.add(message);
                    if (messageBatch != null) {
                        flushRequest(channel, messageBatch);
                    }
                } else {
                    messageBuffer.add(message, false);
                }
            }

            if (messageBuffer.size() >= BATCH_THREASHOLD_WARN) {
                waitForChannelReady();
            }
        }
    }

    public void waitForChannelReady() {
        Channel channel = channelRef.get();
        long pendingTime = 0;
        while (channel == null || !channel.isWritable()) {
            JStormUtils.sleepMs(1);
            pendingTime++;
            if (timeoutMs != -1 && pendingTime >= timeoutMs) {
                LOG.warn("Discard message due to pending message timeout({}ms), messageSize={}", timeoutMs, messageBuffer.size());
                messageBuffer.clear();
                return;
            }
            if (pendingTime % 30000 == 0) {
                LOG.info("Pending total time={}, channel.isWritable={}, remoteAddress={}", pendingTime, 
                		channel != null ? channel.isWritable() : null, channel != null ? channel.getRemoteAddress() : null);
            }
            channel = channelRef.get();
        }

        MessageBatch messageBatch = messageBuffer.drain();
        flushRequest(channel, messageBatch);
    }

    @Override
    public void handleResponse(Channel channel, Object msg) {
    	if (msg == null) {
    		return;
    	}

    	TaskMessage message = (TaskMessage) msg;
    	short type = message.get_type();
        if (type == TaskMessage.BACK_PRESSURE_REQUEST) {
        	byte[] messageData = message.message();
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE + 1);
            buffer.put(messageData);
            buffer.flip();
            boolean startFlowCtrl = buffer.get() == 1 ? true : false;
            int targetTaskId = buffer.getInt();
            //LOG.debug("Received flow ctrl ({}) for target task-{}", startFlowCtrl, targetTaskId);

            Set<Integer> targetTasks = remoteAddrToTasks.get(channel.getRemoteAddress().toString());
            if (targetTasks != null) {
                synchronized (targetTasks) {
            	    if (!targetTasks.contains(targetTaskId)) {
            	        targetTasks.add(targetTaskId);
            	    }
                }
            } else {
            	LOG.warn("TargetTasks set was not initialized correctly!");
            	Set<Integer> taskSet = new HashSet<Integer>();
            	taskSet.add(targetTaskId);
            	remoteAddrToTasks.put(channel.getRemoteAddress().toString(), taskSet);
            }

            try {
            	flowCtrlLock.lock();
                if (startFlowCtrl) {
                    targetTasksUnderFlowCtrl.put(targetTaskId, lock.newCondition());
                    //LOG.debug("Start flow ctrl for target task-{}", targetTaskId);
                } else {
                    Condition condition = targetTasksUnderFlowCtrl.remove(targetTaskId);
                    if (condition != null) {
                    	try {
                    		lock.lock();
                    	    condition.signalAll();
                    	} finally {
                    		lock.unlock();
                    	}
                    }
                
                    MessageBatch cache = null;
                    synchronized (targetTasksCache) {
                    	if(targetTasksCache.get(targetTaskId) != null) {
                    		cache = targetTasksCache.remove(targetTaskId);
                    	}
                    }
                    if (cache != null) {
                    	pushBatch(cache);
                    }
                }
            } finally {
            	flowCtrlLock.unlock();
            }
        } else {
            LOG.warn("Unexpected message (type={}) was received from task {}", type, message.task());
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    private void waitforFlowCtrlAndSend(TaskMessage message) {
    	int targetTaskId = message.task();

    	boolean isSend = true;
    	Condition condition = null;
    	MessageBatch flushCache = null;
    	try {
    		flowCtrlLock.lock();
            condition = targetTasksUnderFlowCtrl.get(targetTaskId);
            // If target task is under flow control
            if (condition != null) {	
        	    MessageBatch cache = targetTasksCache.get(targetTaskId);
        	    if (cache == null) {
        		    cache = new MessageBatch(cacheSize);
        		    targetTasksCache.put(targetTaskId, cache);
        	    }
        		cache.add(message);
        		
        		if (cache.isFull()) {
        			flushCache = targetTasksCache.remove(targetTaskId);
        		} else {
        			isSend = false;
        		}
        	}
    	} finally {
    		flowCtrlLock.unlock();
    	}

    	if (isSend) {
        	// Cache is full. Try to flush till flow control is released.
        	if (flushCache != null) {
        		//LOG.debug("Flow Ctrl: Wait for target task-{}", targetTaskId);
        	    long pendingTime = 0;
        	    boolean done = false;
        	    while (condition != null && !done) {
                	try {
                		lock.lock();
    		    		done = condition.await(flowCtrlAwaitTime, TimeUnit.MILLISECONDS);
    		    		pendingTime += flowCtrlAwaitTime;
    		    		if (timeoutMs != -1 && pendingTime >= timeoutMs) {
    		                LOG.warn("Discard message under flow ctrl due to pending message timeout({}ms), messageSize={}", 
    		                		timeoutMs, flushCache.getEncodedLength());
    		                targetTasksUnderFlowCtrl.remove(targetTaskId);
    		                return;
    		            }
    	          		if (pendingTime % 30000 == 0) {
    	          			LOG.info("Pending total time={} since target task-{} is under flow control ", pendingTime, targetTaskId);
    	          		}
    		    	} catch (InterruptedException e) {
    		    		LOG.info("flow control was interrupted! targetTask-{}", targetTaskId);
    		    	} finally {
    		    		lock.unlock();
    		    	}
                
                	try {
                		flowCtrlLock.lock();
                		condition = targetTasksUnderFlowCtrl.get(targetTaskId);
                	} finally {
                		flowCtrlLock.unlock();
                	}
          	    	
                }

        	    pushBatch(flushCache);
        	} else {
            	pushBatch(message);
            }
        } 
    }

    @Override
    public void connectChannel(Channel channel) {
        remoteAddrToTasks.put(channel.getRemoteAddress().toString(), new HashSet<Integer>());
    }

    private void releaseFlowCtrlsForRemoteAddr(String remoteAddr) {
    	Set<Integer> targetTasks = remoteAddrToTasks.get(remoteAddr);
        if (targetTasks != null) {
        	try {
        		flowCtrlLock.lock();
                for (Integer taskId : targetTasks) {
                    Condition condition = targetTasksUnderFlowCtrl.remove(taskId);
                    if (condition != null) {
                    	try {
                    		lock.lock();
                    	    condition.signalAll();
                    	} finally {
                    		lock.unlock();
                    	}
                    }
                }
        	} finally {
        		flowCtrlLock.unlock();
        	}
        }
    }

    @Override
    public void disconnectChannel(Channel channel) {
    	if (isClosed()) {
            return;
        }

        if (channel == channelRef.get()) {
            setChannel(null);
            releaseFlowCtrlsForRemoteAddr(channel.getRemoteAddress().toString());
            reconnect();
        } else {
        	releaseFlowCtrlsForRemoteAddr(channel.getRemoteAddress().toString());
            closeChannel(channel);
        }
        
    }
}
