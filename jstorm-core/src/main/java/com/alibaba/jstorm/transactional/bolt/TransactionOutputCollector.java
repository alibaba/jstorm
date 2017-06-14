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
package com.alibaba.jstorm.transactional.bolt;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.OutputCollectorCb;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.transactional.BatchSnapshot;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.bolt.TransactionBolt.BatchTracker;
import com.alibaba.jstorm.transactional.utils.CountValue;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (currBatchTracker != null && currBatchTracker.getBatchId() == tracker.getBatchId()) {
            // Do Nothing
        } else {
            flush();
        }
        this.currBatchTracker = tracker;
        delegate.setBatchId(tracker.getBatchId());
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return emit(streamId, null, tuple, new CollectorCallback(currBatchTracker));
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emitDirect(taskId, streamId, null, tuple, new CollectorCallback(currBatchTracker));
    }

    @Override
    public void ack(Tuple input) {

    }

    @Override
    public void fail(Tuple input) {
        long batchId = ((TupleImplExt) input).getBatchId();
        if (batchId > 0)
            bolt.fail(batchId);
        else
            LOG.warn("It's not allowed to fail a tuple with invalid batchId={}", batchId);
    }

    @Override
    public void reportError(Throwable error) {
        delegate.reportError(error);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        return delegate.emit(streamId, null, tuple, callback);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        delegate.emitDirect(taskId, streamId, null, tuple, callback);
    }

    public List<Integer> emitByDelegate(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return emitByDelegate(streamId, null, tuple, null);
    }

    public void emitDirectByDelegate(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emitDirectByDelegate(taskId, streamId, null, tuple, null);
    }

    public List<Integer> emitByDelegate(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        return delegate.emit(streamId, null, tuple, callback);
    }

    public void emitDirectByDelegate(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        delegate.emitDirect(taskId, streamId, null, tuple, callback);
    }

    @Override
    public List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return delegate.emitCtrl(streamId, null, tuple);
    }

    @Override
    public void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        delegate.emitDirectCtrl(taskId, streamId, null, tuple);
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
            int count = entry.getValue().getValueAndSet(0);
            BatchSnapshot snapshot = new BatchSnapshot(currBatchTracker.getBatchId(), count);
            emitDirectByDelegate(taskId, TransactionCommon.BARRIER_STREAM_ID, null, new Values(snapshot), null);
        }
        flush();
    }
}