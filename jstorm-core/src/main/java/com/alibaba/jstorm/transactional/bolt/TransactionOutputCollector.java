package com.alibaba.jstorm.transactional.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.BatchSnapshot;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.bolt.TransactionBolt.BatchTracker;
import com.alibaba.jstorm.transactional.bolt.TransactionBolt.CountValue;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.OutputCollectorCb;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TransactionOutputCollector extends OutputCollectorCb {
    public static Logger LOG = LoggerFactory.getLogger(TransactionOutputCollector.class);

    TransactionBolt bolt;
    OutputCollectorCb delegate;

    private BatchTracker currBatchTracker;

    private class CollectorCallback implements ICollectorCallback {
        private BatchTracker batchTracker;

        public CollectorCallback(BatchTracker tracker) {
            this.batchTracker = tracker;
        }

        @Override
        public void execute(String stream, List<Integer> outTasks, List values) {
            for (int taskId : outTasks) {
                CountValue count = batchTracker.sendMsgCount.get(taskId);
                count.count++;   
            }
        }
    }

    public TransactionOutputCollector(TransactionBolt bolt, OutputCollectorCb delegate) {
        this.bolt = bolt;
        this.delegate = delegate;
    }

    public void setCurrBatchTracker(BatchTracker tracker) {
        if (currBatchTracker != null && currBatchTracker.getBatchGroupId().equals(tracker.getBatchGroupId())) {
            // Do Nothing
        } else {
            flush();
        }
        this.currBatchTracker = tracker;
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return emit(streamId, anchors, tuple, new CollectorCallback(currBatchTracker));
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emitDirect(taskId, streamId, anchors, tuple, new CollectorCallback(currBatchTracker));
    }

    @Override
    public void ack(Tuple input) {
        //delegate.ack(input);
    }

    @Override
    public void fail(Tuple input) {
        BatchGroupId batchGroupId = (BatchGroupId) input.getValue(1);
        if (batchGroupId != null) {
            bolt.fail(batchGroupId);
        } else {
            LOG.warn("Try to fail unexpected message, {}", input);
        }
    }

    @Override
    public void reportError(Throwable error) {
        delegate.reportError(error);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        List<Object> tupleWithId = new ArrayList<Object>();
        tupleWithId.add(new BatchGroupId(currBatchTracker.getBatchGroupId()));
        tupleWithId.addAll(tuple);
        return delegate.emit(streamId, anchors, tupleWithId, callback);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        List<Object> tupleWithId = new ArrayList<Object>();
        tupleWithId.add(new BatchGroupId(currBatchTracker.getBatchGroupId()));
        tupleWithId.addAll(tuple);
        delegate.emitDirect(taskId, streamId, anchors, tupleWithId, callback);
    }

    public List<Integer> emitByDelegate(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return emitByDelegate(streamId, anchors, tuple, null);
    }

    public void emitDirectByDelegate(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emitDirectByDelegate(taskId, streamId, anchors, tuple, null);
    }

    public List<Integer> emitByDelegate(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        return delegate.emit(streamId, anchors, tuple, callback);
    }

    public void emitDirectByDelegate(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        delegate.emitDirect(taskId, streamId, anchors, tuple, callback);
    }

    public void flush() {
        delegate.flush();
    }

    public void flushBarrier() {
        // flush pending message in outputCollector
        flush();
        
        // Emit and flush barrier message to all downstream tasks
        for (Entry<Integer, CountValue> entry : currBatchTracker.sendMsgCount.entrySet()) {
            int taskId = entry.getKey();
            CountValue count = entry.getValue();
            BatchSnapshot snapshot = new BatchSnapshot(currBatchTracker.getBatchGroupId(), count.count);
            emitDirectByDelegate(taskId, TransactionCommon.BARRIER_STREAM_ID, null, 
                    new Values(new BatchGroupId(currBatchTracker.getBatchGroupId()), snapshot), null);
            count.count = 0;
        }
        flush();   
    }
}