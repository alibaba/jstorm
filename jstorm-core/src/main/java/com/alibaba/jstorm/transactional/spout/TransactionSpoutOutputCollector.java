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
package com.alibaba.jstorm.transactional.spout;

import backtype.storm.spout.SpoutOutputCollectorCb;
import backtype.storm.task.ICollectorCallback;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.transactional.BatchSnapshot;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.utils.CountValue;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionSpoutOutputCollector extends SpoutOutputCollectorCb {
    public static Logger LOG = LoggerFactory.getLogger(TransactionSpoutOutputCollector.class);

    SpoutOutputCollectorCb delegate;
    private TransactionSpout spout;

    private long currBatchId;
    private Map<Integer, CountValue> msgCount;

    private class CollectorCallback implements ICollectorCallback {
        @Override
        public void execute(String stream, List<Integer> outTasks, List values) {
            for (Integer task : outTasks) {
                CountValue count = msgCount.get(task);
                count.count++;
            }
        }
    }

    public TransactionSpoutOutputCollector(SpoutOutputCollectorCb delegate, TransactionSpout spout) {
        this.delegate = delegate;
        this.spout = spout;
    }

    public void init(long batchId, Set<Integer> targetTasks) {
        setCurrBatchId(batchId);
        initMsgCount(targetTasks);
        LOG.info("Init output: batchId={}", currBatchId);
    }

    public void initMsgCount(Set<Integer> targetTasks) {
        this.msgCount = new HashMap<>();
        for (Integer task : targetTasks) {
            this.msgCount.put(task, new CountValue());
        }
    }

    public void setCurrBatchId(long batchId) {
        this.currBatchId = batchId;
        delegate.setBatchId(batchId);
    }

    public long getCurrBatchId() {
        return currBatchId;
    }

    public void waitActive() {
        while (!spout.isActive()) {
            JStormUtils.sleepMs(1);
        }
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return emit(streamId, tuple, null, new CollectorCallback());
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitDirect(taskId, streamId, tuple, null, new CollectorCallback());
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        delegate.emit(streamId, tuple, null, (callback != null) ? callback : new CollectorCallback());
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        delegate.emitDirect(taskId, streamId, tuple, null, (callback != null) ? callback : new CollectorCallback());
    }

    public List<Integer> emitByDelegate(String streamId, List<Object> tuple, Object messageId) {
        return emitByDelegate(streamId, tuple, null, null);
    }

    public List<Integer> emitByDelegate(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        return delegate.emit(streamId, tuple, null, callback);
    }

    public void emitDirectByDelegate(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitDirectByDelegate(taskId, streamId, tuple, null, null);
    }

    public void emitDirectByDelegate(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        delegate.emitDirect(taskId, streamId, tuple, null, callback);
    }

    @Override
    public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple, Object messageId) {
        delegate.emitDirectCtrl(taskId, streamId, tuple, null);
    }

    @Override
    public List<Integer> emitCtrl(String streamId, List<Object> tuple, Object messageId) {
        return delegate.emitCtrl(streamId, tuple, null);
    }

    @Override
    public void reportError(Throwable error) {
        delegate.reportError(error);
    }

    @Override
    public void emitBarrier() {
        spout.commit();
    }

    public int flushBarrier() {
        delegate.flush();

        int ret = 0;
        // Emit and flush barrier message to all downstream tasks
        for (Entry<Integer, CountValue> entry : msgCount.entrySet()) {
            int taskId = entry.getKey();
            int count = entry.getValue().getValueAndSet(0);
            ret += count;

            BatchSnapshot barrierSnapshot = new BatchSnapshot(currBatchId, count);
            emitDirectByDelegate(taskId, TransactionCommon.BARRIER_STREAM_ID, new Values(barrierSnapshot), null, null);
        }
        delegate.flush();

        return ret;
    }

    public void moveToNextBatch() {
        currBatchId++;
        delegate.setBatchId(currBatchId);
    }

    public void flushInitBarrier() {
        // flush pending message in outputCollector
        delegate.flush();
        delegate.setBatchId(TransactionCommon.INIT_BATCH_ID);
        BatchSnapshot barrierSnapshot = new BatchSnapshot(TransactionCommon.INIT_BATCH_ID, 0);
        for (Entry<Integer, CountValue> entry : msgCount.entrySet()) {
            entry.getValue().setValue(0);
            emitDirectByDelegate(entry.getKey(), TransactionCommon.BARRIER_STREAM_ID, new Values(barrierSnapshot), null, null);
        }
        delegate.flush();
        delegate.setBatchId(currBatchId);
    }

}