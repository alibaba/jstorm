package com.alibaba.jstorm.transactional.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.BatchSnapshot;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.spout.SpoutOutputCollectorCb;
import backtype.storm.task.ICollectorCallback;
import backtype.storm.tuple.Values;

public class TransactionSpoutOutputCollector extends SpoutOutputCollectorCb {
    public static Logger LOG = LoggerFactory.getLogger(TransactionSpoutOutputCollector.class);

    SpoutOutputCollectorCb delegate;
    private TransactionSpout spout;

    private int groupId;
    private long currBatchId;
    private Map<Integer, Integer> msgCount;
    private BatchInfo currBatchInfo;

    private ReadWriteLock lock;

    public static class BatchInfo {
        public long batchId;
        public Object endPos;

        public BatchInfo() {
            
        }

        public BatchInfo(BatchInfo info) {
            this.batchId = info.batchId;
            this.endPos = info.endPos;
        }

        public void init(long batchId) {
            this.batchId = batchId;
            this.endPos = null;
        }
    }

    private class CollectorCallback implements ICollectorCallback {

        @Override
        public void execute(String stream, List<Integer> outTasks, List values) {
            for (Integer task : outTasks) {
                int count = msgCount.get(task);
                msgCount.put(task, ++count);
            }
        }

    }

    public TransactionSpoutOutputCollector(SpoutOutputCollectorCb delegate, TransactionSpout spout) {
        this.delegate = delegate;
        this.lock = new ReentrantReadWriteLock();
        this.currBatchInfo = new BatchInfo();
        this.spout = spout;
    }

    public void init(BatchGroupId id, Set<Integer> targetTasks) {
        try {
            lock.writeLock().lock();
            setGroupId(id.groupId);
            setCurrBatchId(id.batchId);
            initMsgCount(targetTasks);
            currBatchInfo.init(id.batchId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void initMsgCount(Set<Integer> targetTasks) {
        this.msgCount = new HashMap<Integer, Integer>();
        for (Integer task : targetTasks) {
            this.msgCount.put(task, 0);
        }
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setCurrBatchId(long batchId) {
        this.currBatchId = batchId;
    }

    public long getCurrBatchId() {
        return currBatchId;
    }

    public void waitActive() {
        while (spout.isActive() == false) {
            JStormUtils.sleepMs(1);
        }
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return emit(streamId, tuple, messageId, new CollectorCallback());
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitDirect(taskId, streamId, tuple, messageId, new CollectorCallback()); 
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        try {
            //waitActive();
            lock.readLock().lock();
            List<Object> tupleWithId = new ArrayList<Object>();
            tupleWithId.add(new BatchGroupId(groupId, currBatchId));
            tupleWithId.addAll(tuple);
            delegate.emit(streamId, tupleWithId, null, (callback != null) ? callback : new CollectorCallback());
            //currBatchInfo.endPos = messageId;
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        try {
            //waitActive();
            lock.readLock().lock();
            List<Object> tupleWithId = new ArrayList<Object>();
            tupleWithId.add(new BatchGroupId(groupId, currBatchId));
            tupleWithId.addAll(tuple);
            delegate.emitDirect(taskId, streamId, tupleWithId, null, (callback != null) ? callback : new CollectorCallback());
            //currBatchInfo.endPos = messageId;
        } finally {
            lock.readLock().unlock();
        }   
    }

    public List<Integer> emitByDelegate(String streamId, List<Object> tuple, Object messageId) {
        return emitByDelegate(streamId, tuple, messageId, null);
    }

    public List<Integer> emitByDelegate(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        return delegate.emit(streamId, tuple, messageId, callback);
    }

    public void emitDirectByDelegate(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitDirectByDelegate(taskId, streamId, tuple, messageId, null);
    }

    public void emitDirectByDelegate(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        delegate.emitDirect(taskId, streamId, tuple, messageId, callback);
    }

    @Override
    public void reportError(Throwable error) {
        delegate.reportError(error);
    }

    public BatchInfo flushBarrier() {
        BatchInfo ret = null;
        try {
            lock.writeLock().lock();
            ret = new BatchInfo(currBatchInfo);
            // flush pending message in outputCollector
            delegate.flush();

            // Emit and flush barrier message to all downstream tasks
            BatchGroupId batchGroupId = new BatchGroupId(groupId, currBatchId);
            for (Entry<Integer, Integer> entry : msgCount.entrySet()) {
                int taskId = entry.getKey();
                int count = entry.setValue(0);   
                BatchSnapshot barrierSnapshot = new BatchSnapshot(batchGroupId, count);
                emitDirectByDelegate(taskId, TransactionCommon.BARRIER_STREAM_ID, new Values(batchGroupId, barrierSnapshot), null, null);
            }
            delegate.flush();
            moveToNextBatch();
        } finally {
            lock.writeLock().unlock();
        }

        return ret;
    }

    public void moveToNextBatch() {
        currBatchId++;
        currBatchInfo.batchId = currBatchId;
    }

    public void flushInitBarrier() {
        try {
            lock.writeLock().lock();
            // flush pending message in outputCollector
            delegate.flush();
            BatchGroupId batchGroupId = new BatchGroupId(groupId, TransactionCommon.INIT_BATCH_ID);
            BatchSnapshot barrierSnapshot = new BatchSnapshot(batchGroupId, 0);
            for (Entry<Integer, Integer> entry : msgCount.entrySet()) {
                entry.setValue(0);   
                emitDirectByDelegate(entry.getKey(), TransactionCommon.BARRIER_STREAM_ID, new Values(batchGroupId, barrierSnapshot), null, null);                
            }
            delegate.flush();
        } finally {
            lock.writeLock().unlock();
        }
    }
}