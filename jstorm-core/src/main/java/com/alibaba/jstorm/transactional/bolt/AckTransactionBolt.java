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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.utils.AckPendingBatchTracker;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.OutputCollectorCb;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.Utils;

/**
 * This bolt was created to provide compatibility with the ACK mechanism of Storm
 */
public class AckTransactionBolt implements ITransactionStatefulBoltExecutor {
    private static final long serialVersionUID = 807846832890753253L;

    private static Logger LOG = LoggerFactory.getLogger(AckTransactionBolt.class);

    private IRichBolt bolt;
    private TaskBaseMetric taskStats;
    private String componentId;
    private int taskId;

    private AckOutputCollector ackOutputCollector;
    private AckPendingBatchTracker<Pair<Long, Integer>> batchXorTracker;
    private long batchTimeout;
    private Random random;

    private class AckOutputCollector extends OutputCollectorCb {
        private OutputCollectorCb delegate;
        private AckPendingBatchTracker<Pair<Long, Integer>> tracker;

        public AckOutputCollector(OutputCollectorCb delegate, AckPendingBatchTracker<Pair<Long, Integer>> tracker) {
            this.delegate = delegate;
            this.tracker = tracker;
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            if (anchors != null) {
                tuple.add(Utils.generateId(random));
            } else {
                tuple.add(0l);
            }
            return delegate.emit(streamId, null, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            if (anchors != null) {
                tuple.add(Utils.generateId(random));
            } else {
                tuple.add(0l);
            }
            delegate.emitDirect(taskId, streamId, null, tuple);
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
            if (anchors != null) {
                tuple.add(Utils.generateId(random));
            } else {
                tuple.add(0l);
            }
            return delegate.emit(streamId, null, tuple, callback);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
            if (anchors != null) {
                tuple.add(Utils.generateId(random));
            } else {
                tuple.add(0l);
            }
            delegate.emitDirect(taskId, streamId, null, tuple, callback);
        }

        @Override
        public void ack(Tuple input) {
            Pair<Long, Integer> pendingBatch = tracker.getPendingBatch(((TupleImplExt) input).getBatchId(), input.getSourceStreamId());
            if (pendingBatch != null) {
                long rootId = getRootId(input);
                if (rootId != 0)
                    pendingBatch.setFirst(JStormUtils.bit_xor(pendingBatch.getFirst(), rootId));
            }
            delegate.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            delegate.fail(input);
        }

        @Override
        public void reportError(Throwable error) {
            delegate.reportError(error);
        }

        @Override
        public List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            return delegate.emitCtrl(streamId, anchors, tuple);
        }

        @Override
        public void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            delegate.emitDirectCtrl(taskId, streamId, anchors, tuple);
        }
    }

    public AckTransactionBolt(IRichBolt bolt) {
        this.bolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.batchXorTracker = new AckPendingBatchTracker<>();
        this.ackOutputCollector = new AckOutputCollector(collector.getDelegate(), batchXorTracker);
        this.bolt.prepare(stormConf, context, new OutputCollector(ackOutputCollector));

        this.componentId = context.getThisComponentId();
        this.taskId = context.getThisTaskId();
        this.taskStats = new TaskBaseMetric(context.getTopologyId(), componentId, taskId);
        this.batchTimeout = ConfigExtension.getTransactionBatchSnapshotTimeout(stormConf) * 1000;
        this.random = new Random(Utils.secureRandomLong());

        LOG.info("batchTimeout: {}", batchTimeout);
    }

    private long getRootId(Tuple input) {
        int rootIdIndex = input.getValues().size() - 1;
        return (long) input.getValue(rootIdIndex);
    }

    @Override
    public void execute(Tuple input) {
        long rootId = getRootId(input);
        if (rootId != 0) {
            long batchId = ((TupleImplExt) input).getBatchId();
            String streamId = input.getSourceStreamId();
            Pair<Long, Integer> pendingBatch = batchXorTracker.getPendingBatch(batchId, streamId, true);
            if (pendingBatch == null) {
                pendingBatch = new Pair<>(0l, 0);
                batchXorTracker.putPendingBatch(batchId, streamId, pendingBatch);
            }
            pendingBatch.setFirst(JStormUtils.bit_xor(pendingBatch.getFirst(), rootId));
            int count = pendingBatch.getSecond();
            pendingBatch.setSecond(++count);
        }
        bolt.execute(input);
    }

    @Override
    public void cleanup() {
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    @Override
    public Object finishBatch(long batchId) {
        return batchId;
    }

    @Override
    public void initState(Object userState) {

    }

    @Override
    public Object commit(long batchId, Object state) {
        Set<String> unfinishStreams = batchXorTracker.getStreamIds(batchId);
        long waitTime = 0;
        checkIfStreamsFinish(batchId, unfinishStreams);
        while (unfinishStreams.size() > 0 && waitTime < batchTimeout) {
            JStormUtils.sleepMs(1);
            waitTime++;
            checkIfStreamsFinish(batchId, unfinishStreams);
        }

        if (unfinishStreams.size() != 0 && waitTime >= batchTimeout) {
            LOG.info("Timeout for acking. waitTime={}", waitTime);
            failBatch(batchId, unfinishStreams);
            return TransactionCommon.COMMIT_FAIL;
        } else {
            return batchId;
        }
    }

    private void checkIfStreamsFinish(long batchId, Set<String> streams) {
        Iterator<String> itr = streams.iterator();
        while (itr.hasNext()) {
            String streamId = itr.next();
            Pair<Long, Integer> pendingBatch = batchXorTracker.getPendingBatch(batchId, streamId);
            if (pendingBatch == null || pendingBatch.getFirst() == 0) {
                if (pendingBatch != null) {
                    taskStats.bolt_acked_tuple(componentId, streamId, pendingBatch.getSecond());
                    taskStats.update_bolt_acked_latency(componentId, streamId, batchXorTracker.getBatchStartTime(batchId), System.currentTimeMillis(),
                            pendingBatch.getSecond());
                }
                itr.remove();
            }
        }
    }

    private void failBatch(long batchId, Set<String> streams) {
        for (String streamId : streams) {
            Pair<Long, Integer> pendingBatch = batchXorTracker.getPendingBatch(batchId, streamId);
            if (pendingBatch != null && pendingBatch.getSecond() > 0)
                taskStats.boltFailedTuple(componentId, streamId, pendingBatch.getSecond());
        }
    }

    @Override
    public void rollBack(Object userState) {
        LOG.info("Ack pending batch tracker: {}", batchXorTracker);
        batchXorTracker.clear();
    }

    @Override
    public void ackCommit(long batchId, long timeStamp) {
        LOG.debug("Receive ack commit for batch-{}", batchId);
        removeObsoleteBatches(batchId);
        batchXorTracker.removeBatch(batchId);
    }

    private void removeObsoleteBatches(long commitBatchId) {
        TreeSet<Long> totalBatches = new TreeSet<Long>(batchXorTracker.getBatchIds());
        Set<Long> obsoleteBatches = totalBatches.headSet(commitBatchId);
        if (obsoleteBatches != null && obsoleteBatches.size() > 0) {
            LOG.info("Remove obsolete batches: {}", obsoleteBatches);
            for (Long batchId : obsoleteBatches) {
                batchXorTracker.removeBatch(batchId);
            }
        }
    }
}