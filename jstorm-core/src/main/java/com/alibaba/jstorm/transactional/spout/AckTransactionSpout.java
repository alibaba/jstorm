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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.transactional.utils.AckPendingBatchTracker;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollectorCb;
import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;

/**
 * This spout was created to provide compatibility with the ACK mechanism of Storm
 */
public class AckTransactionSpout implements ITransactionSpoutExecutor {
    private static final long serialVersionUID = -6561817670963028414L;

    private static Logger LOG = LoggerFactory.getLogger(AckTransactionSpout.class);

    private IRichSpout spoutExecutor;
    private boolean isCacheTuple;
    private TaskBaseMetric taskStats;

    private volatile long currBatchId = -1;
    // Map<BatchId, Map<StreamId, Map<MsgId, Value>>>
    private AckPendingBatchTracker<Map<Object, List<Object>>> tracker;

    private Random random;

    private class AckSpoutOutputCollector extends SpoutOutputCollectorCb {
        private SpoutOutputCollectorCb delegate;

        public AckSpoutOutputCollector (SpoutOutputCollectorCb delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            if (messageId != null) {
                addPendingTuple(currBatchId, streamId, messageId, tuple);
                tuple.add(Utils.generateId(random));
            } else {
                // for non-anchor tuples, use 0 as default rootId
                tuple.add(0l);
            }
            return delegate.emit(streamId, tuple, null);
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            delegate.emitDirect(taskId, streamId, tuple, null);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
            if (messageId != null) {
                addPendingTuple(currBatchId, streamId, messageId, tuple);
                tuple.add(Utils.generateId(random));
            } else {
                // for non-anchor tuples, use 0 as default rootId
                tuple.add(0l);
            }
            return delegate.emit(streamId, tuple, null, callback);
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
            if (messageId != null) {
                addPendingTuple(currBatchId, streamId, messageId, tuple);
                tuple.add(Utils.generateId(random));
            } else {
                tuple.add(0l);
            }
            delegate.emitDirect(taskId, streamId, tuple, null, callback);
        }

        @Override
        public void reportError(Throwable error) {
            delegate.reportError(error);
        }

        @Override
        public void flush(){
            delegate.flush();
        }

        @Override
        public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple, Object messageId) {
            delegate.emitDirectCtrl(taskId, streamId, tuple, messageId);
        }

        @Override
        public List<Integer> emitCtrl(String streamId, List<Object> tuple, Object messageId) {
            return delegate.emitCtrl(streamId, tuple, messageId);
        }
    }

    public AckTransactionSpout(IRichSpout spout) {
        this.spoutExecutor = spout;
        if (spoutExecutor instanceof IAckValueSpout || spoutExecutor instanceof IFailValueSpout)
            isCacheTuple = true;
        else
            isCacheTuple = false;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        SpoutOutputCollectorCb ackOutput = new AckSpoutOutputCollector(collector.getDelegate());
        spoutExecutor.open(conf, context, new SpoutOutputCollector(ackOutput));
        tracker = new AckPendingBatchTracker<>();
        taskStats = new TaskBaseMetric(context.getTopologyId(), context.getThisComponentId(), context.getThisTaskId());
        random = new Random(Utils.secureRandomLong());
    }

    @Override
    public void close() {
        spoutExecutor.close();
    }

    @Override
    public void activate() {
        spoutExecutor.activate();
    }

    @Override
    public void deactivate() {
        spoutExecutor.deactivate();
    }

    @Override
    public void nextTuple() {
        spoutExecutor.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        spoutExecutor.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        spoutExecutor.fail(msgId);
    }

    public void ack(Object msgId, List<Object> values) {
        ((IAckValueSpout) spoutExecutor).ack(msgId, values);
    }

    public void fail(Object msgId, List<Object> values) {
        ((IFailValueSpout) spoutExecutor).fail(msgId, values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        spoutExecutor.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spoutExecutor.getComponentConfiguration();
    }

    @Override
    public void initState(Object userState) {
        if (userState != null) {
            currBatchId = (Long) userState;
        } else {
            currBatchId = 1;
        }
    }

    @Override
    public Object finishBatch(long batchId) {
        currBatchId++;
        return null;
    }

    @Override
    public Object commit(long batchId, Object state) {
        return batchId;
    }

    @Override
    public void rollBack(Object userState) {
        if (userState != null) {
            currBatchId = (Long) userState;
        } else {
            currBatchId = 1;
        }

        removeObsoleteBatches(currBatchId);
        for (Long batchId : tracker.getBatchIds())
            ackOrFailBatch(batchId, false);
    }

    @Override
    public void ackCommit(long batchId, long timeStamp) {
        ackOrFailBatch(batchId, true);
        removeObsoleteBatches(batchId);
    }

    private void ackOrFailBatch(long batchId, boolean isAck) {
        for (String streamId : tracker.getStreamIds(batchId)) {
            Map<Object, List<Object>> pendingBatch = tracker.getPendingBatch(batchId, streamId);
            if (pendingBatch == null)
                continue;

            for (Entry<Object, List<Object>> entry : pendingBatch.entrySet()) {
                ackOrFailTuple(entry.getKey(), entry.getValue(), isAck);
            }
            if (isAck)
                taskStats.spoutAckedTuple(streamId, pendingBatch.size());
            else
                taskStats.spoutFailedTuple(streamId, pendingBatch.size());
        }
    }

    private void ackOrFailTuple(Object msgId, List<Object> value, boolean isAck) {
        if (isAck) {
            if (spoutExecutor instanceof IAckValueSpout)
                ack(msgId, value);
            else
                ack(msgId);
        } else {
            if (spoutExecutor instanceof IFailValueSpout)
                fail(msgId, value);
            else
                fail(msgId);
        }
    }

    private void addPendingTuple(long batchId, String streamId, Object msgId, List<Object> value) {
        Map<Object, List<Object>> pendingBatch = tracker.getPendingBatch(batchId, streamId);
        if (pendingBatch == null) {
            pendingBatch = new HashMap<>();
        }
        List<Object> cacheValue = isCacheTuple ? value : null;
        pendingBatch.put(msgId, cacheValue);
    }

    private void removeObsoleteBatches(long commitBatchId) {
        TreeSet<Long> totalBatches = new TreeSet<Long>(tracker.getBatchIds());
        Set<Long> obsoleteBatches = totalBatches.headSet(commitBatchId);
        if (obsoleteBatches != null && obsoleteBatches.size() > 0) {
            LOG.info("Remove obsolete batches: {}", obsoleteBatches);
            for (Long batchId : obsoleteBatches) {
                tracker.removeBatch(batchId);
            }
        }
    }
}
