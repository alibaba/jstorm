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

import backtype.storm.Config;
import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.spout.ICtrlMsgSpout;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.TransactionOutputFieldsDeclarer;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionSpout implements IRichSpout, ICtrlMsgSpout {
    private static final long serialVersionUID = 8289040804669685438L;

    public static Logger LOG = LoggerFactory.getLogger(TransactionSpout.class);

    protected enum Operation {
        nextTuple, ctrlEvent, commit
    }

    protected Map conf;
    protected TopologyContext topologyContext;
    protected String topologyId;
    protected int taskId;
    protected int topologyMasterId;
    protected String componentId;
    protected TaskBaseMetric taskStats;
    protected Set<Integer> downstreamTasks;

    protected ITransactionSpoutExecutor spoutExecutor;
    protected TransactionSpoutOutputCollector outputCollector;

    protected volatile State spoutStatus;

    protected IntervalCheck initRetryCheck;

    protected int groupId;
    protected TransactionState currState;

    // Map<BatchId, Pair<BatchSize, TimeStamp>>
    protected SortedMap<Long, Pair<Integer, Long>> committingBatches;
    protected volatile boolean isMaxPending;
    protected int MAX_PENDING_BATCH_NUM;

    protected Lock lock;

    public TransactionSpout(ITransactionSpoutExecutor spoutExecutor) {
        this.spoutExecutor = spoutExecutor;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.topologyContext = context;
        this.topologyId = topologyContext.getTopologyId();
        this.taskId = topologyContext.getThisTaskId();
        this.topologyMasterId = topologyContext.getTopologyMasterId();
        this.componentId = topologyContext.getThisComponentId();
        this.taskStats = new TaskBaseMetric(topologyId, componentId, taskId);
        this.downstreamTasks = TransactionCommon.getDownstreamTasks(componentId, topologyContext);
        LOG.info("downstreamTasks: {}", downstreamTasks);

        this.outputCollector = new TransactionSpoutOutputCollector(collector, this);

        this.spoutStatus = State.INIT;
        this.committingBatches = new TreeMap<>();
        this.isMaxPending = false;
        this.MAX_PENDING_BATCH_NUM = ConfigExtension.getTransactionMaxPendingBatch(conf);

        int taskLaunchTimeout = JStormUtils.parseInt(conf.get(Config.NIMBUS_TASK_LAUNCH_SECS));
        int spoutInitRetryDelaySec = JStormUtils.parseInt(conf.get("transaction.spout.init.retry.secs"), taskLaunchTimeout);
        this.initRetryCheck = new IntervalCheck();
        initRetryCheck.setInterval(spoutInitRetryDelaySec);

        this.lock = new ReentrantLock(true);
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
        process(Operation.nextTuple, null);
    }

    @Override
    public void ack(Object msgId) {
        LOG.error("This ack method should not be called.");
    }

    @Override
    public void fail(Object msgId) {
        LOG.error("This fail method should not be called.");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        spoutExecutor.declareOutputFields(declarer);
        declarer.declareStream(TransactionCommon.BARRIER_STREAM_ID, new Fields(TransactionCommon.BARRIER_SNAPSHOT_FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spoutExecutor.getComponentConfiguration();
    }

    @Override
    public void procCtrlMsg(TopoMasterCtrlEvent event) {
        process(Operation.ctrlEvent, event);
    }

    protected void process(Operation op, Object event) {
        try {
            lock.lock();
            if (op.equals(Operation.nextTuple)) {
                doNextTuple();
            } else if (op.equals(Operation.commit)) {
                commit();
            } else if (op.equals(Operation.ctrlEvent)) {
                processCtrlEvent((TopoMasterCtrlEvent) event);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void doNextTuple() {
        if (spoutStatus.equals(State.INIT)) {
            if (!initRetryCheck.isStart()) {
                initRetryCheck.start();
                startInitState();
            } else {
                if (initRetryCheck.check()) {
                    startInitState();
                }
            }
            JStormUtils.sleepMs(10);
        } else {
            if (isActive()) {
                spoutExecutor.nextTuple();
            } else {
                JStormUtils.sleepMs(1);
            }
        }
    }

    protected void processCtrlEvent(TopoMasterCtrlEvent event) {
        // LOG.info("Received contronl event, {}", event.toString());
        TransactionState state = null;
        switch (event.getEventType()) {
            case transactionInitState:
                if (spoutStatus.equals(State.INIT)) {
                    if (event.hasEventValue()) {
                        state = (TransactionState) event.getEventValue().get(0);
                    }
                    initSpoutState(state);
                    spoutStatus = State.ACTIVE;
                }
                break;
            case transactionRollback:
                spoutStatus = State.ROLLBACK;
                if (event.hasEventValue()) {
                    state = (TransactionState) event.getEventValue().get(0);
                }
                LOG.info("Rollback to state, {}", state);
                rollbackSpoutState(state);
                JStormUtils.sleepMs(5000);
                outputCollector.flushInitBarrier();
                break;
            case transactionCommit:
                long successBatchId = (long) event.getEventValue().get(0);
                removeSuccessBatch(successBatchId);
                // LOG.info("Commit Acked, current pending batchs: {}", committingBatches);
                break;
            case transactionStop:
                spoutStatus = State.INACTIVE;
                LOG.info("Stop, current pending batches: {}", committingBatches);
                break;
            case transactionStart:
                spoutStatus = State.ACTIVE;
                LOG.info("Start, current pending batches: {}", committingBatches);
                break;
            default:
                LOG.warn("Received unsupported event, {}", event.toString());
                break;
        }
    }

    public boolean isActive() {
        return (!isMaxPending) && spoutStatus.equals(State.ACTIVE);
    }

    protected void startInitState() {
        LOG.info("Start to retrieve the initial state from topology master");
        // retrieve state from topology master
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionInitState);
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(event), null);
    }

    private void initSpoutState(TransactionState state) {
        LOG.info("Initial state for spout: {}", state);
        if (state == null) {
            currState = new TransactionState(TransactionCommon.INIT_BATCH_ID, null, null);
        } else {
            currState = new TransactionState(state);
        }
        spoutExecutor.initState(currState.getUserCheckpoint());
        resetSpoutState();
    }

    private void rollbackSpoutState(TransactionState state) {
        if (state != null) {
            currState = new TransactionState(state);
            spoutExecutor.rollBack(state.getUserCheckpoint());
        }
        resetSpoutState();
    }

    private void resetSpoutState() {
        outputCollector.init(currState.getCurrBatchId(), downstreamTasks);
        committingBatches.clear();
        moveToNextBatch();
    }

    private void fail(long batchId) {
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionRollback);
        event.addEventValue(batchId);
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(event), null);
    }

    protected void commit() {
        if (!isActive()) {
            // If spout has not finished initialization or is inactive, just return.
            return;
        }

        // commit operation of user
        long batchId = outputCollector.getCurrBatchId();
        Object userState = spoutExecutor.finishBatch(batchId);
        Object commitState;
        try {
            commitState = spoutExecutor.commit(batchId, userState);
        } catch (Exception e) {
            LOG.warn("Failed to commit spout state for batch-{}, {}", batchId, e);
            fail(batchId);
            return;
        }
        if (commitState == TransactionCommon.COMMIT_FAIL) {
            LOG.warn("Failed to commit spout state for batch-{}", batchId);
            fail(batchId);
            return;
        }

        // Send commit request to topology master
        currState.setBatchId(batchId);
        currState.setUserCheckpoint(commitState);

        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionCommit);
        TransactionState state = new TransactionState(currState);
        event.addEventValue(state.getCurrBatchId());
        event.addEventValue(state);
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(event), null);

        // send barrier to downstream tasks
        int batchSize = outputCollector.flushBarrier();
        committingBatches.get(batchId).setFirst(batchSize);

        moveToNextBatch();
    }

    private void moveToNextBatch() {
        outputCollector.moveToNextBatch();
        committingBatches.put(outputCollector.getCurrBatchId(), new Pair(0, System.currentTimeMillis()));
        updateMaxPendingFlag();
    }

    private void removeSuccessBatch(long successBatchId) {
        SortedMap<Long, Pair<Integer, Long>> obsoleteBatches = committingBatches.headMap(successBatchId);
        if (obsoleteBatches.size() > 0) {
            LOG.info("Obsolete batchIds are {}, successBatchId is {}", obsoleteBatches, successBatchId);
            for (Long batchId : obsoleteBatches.keySet()) {
                committingBatches.remove(batchId);
            }
        }

        Pair<Integer, Long> commitBatchInfo = committingBatches.remove(successBatchId);
        if (commitBatchInfo == null) {
            LOG.warn("Batch-{} has already been removed", successBatchId);
        } else {
            if (commitBatchInfo.getFirst() > 0)
                taskStats.spoutProcessLatency("", commitBatchInfo.getSecond(), System.currentTimeMillis(), commitBatchInfo.getFirst());
        }
        updateMaxPendingFlag();
    }

    private void updateMaxPendingFlag() {
        if (MAX_PENDING_BATCH_NUM == 0) {
            isMaxPending = false;
        } else {
            if (committingBatches.size() <= MAX_PENDING_BATCH_NUM) {
                isMaxPending = false;
            } else {
                isMaxPending = true;
            }
        }
    }
}