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
package com.alibaba.jstorm.transactional.state;

import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.execute.BoltCollector;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.state.TransactionState.State;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotStateMaster {
    private static final Logger LOG = getLogger(SnapshotStateMaster.class);

    private String topologyId;
    private String topologyName;
    private StormTopology topology;
    private Map conf;
    private OutputCollector outputCollector;
    private TopologyContext context;

    private Map<String, Set<Integer>> spouts;
    private Map<String, Set<Integer>> statefulBolts;
    private Map<String, Set<Integer>> nonStatefulBoltTasks;
    private Set<Integer> endBoltTasks;
    private ITopologyStateOperator stateOperator;
    private SnapshotState snapshotState;

    private int batchSnapshotTimeout;
    private ScheduledExecutorService scheduledService = null;

    private Lock lock;

    public SnapshotStateMaster(TopologyContext context, OutputCollector outputCollector) {
        this.topologyId = context.getTopologyId();
        try {
            this.topologyName = Common.topologyIdToName(topologyId);
        } catch (InvalidTopologyException e) {
            LOG.error("Failed to convert topologyId to topologyName", e);
            throw new RuntimeException(e);
        }
        this.topology = context.getRawTopology();
        this.conf = context.getStormConf();
        this.outputCollector = outputCollector;
        this.context = context;

        String topologyStateOpClassName = ConfigExtension.getTopologyStateOperatorClass(conf);
        if (topologyStateOpClassName == null) {
            stateOperator = new DefaultTopologyStateOperator();
        } else {
            stateOperator = (ITopologyStateOperator) Utils.newInstance(topologyStateOpClassName);
        }
        stateOperator.init(context);

        Set<String> spoutIds = topology.get_spouts().keySet();
        Set<String> statefulBoltIds = TransactionCommon.getStatefulBolts(topology);
        Set<String> endBolts = TransactionCommon.getEndBolts(topology);
        Set<String> downstreamComponents = new HashSet<>(topology.get_bolts().keySet());

        spouts = componentToComponentTasks(context, spoutIds);
        statefulBolts = componentToComponentTasks(context, statefulBoltIds);
        downstreamComponents.removeAll(statefulBoltIds);
        nonStatefulBoltTasks = componentToComponentTasks(context, downstreamComponents);
        endBoltTasks = new HashSet<Integer>(context.getComponentsTasks(endBolts));
        snapshotState = new SnapshotState(context, spouts, statefulBolts, nonStatefulBoltTasks, endBoltTasks, stateOperator);

        SnapshotState commitState = ConfigExtension.resetTransactionTopologyState(conf) ? null : (SnapshotState) stateOperator.initState(topologyName);
        snapshotState.initState(commitState);

        LOG.info("topologySnapshotState: {}, isResetTopologyState: {}", snapshotState, ConfigExtension.resetTransactionTopologyState(conf));
        LOG.info("lastSuccessfulSnapshotState: {}", snapshotState.getLastSuccessfulBatch().statesInfo());

        this.batchSnapshotTimeout = ConfigExtension.getTransactionBatchSnapshotTimeout(conf);
        scheduledService = Executors.newSingleThreadScheduledExecutor();
        scheduledService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                expiredCheck();
            }
        }, batchSnapshotTimeout, batchSnapshotTimeout / 2, TimeUnit.SECONDS);

        this.lock = new ReentrantLock(true);
    }

    private Map<String, Set<Integer>> componentToComponentTasks(TopologyContext context, Set<String> components) {
        Map<String, Set<Integer>> ret = new HashMap<>();
        for (String component : components) {
            ret.put(component, new HashSet<Integer>(context.getComponentTasks(component)));
        }
        return ret;
    }

    public void process(Tuple tuple) {
        try {
            lock.lock();
            processTuple(tuple);
        } finally {
            lock.unlock();
        }
    }

    public void processTuple(Tuple tuple) {
        int taskId = tuple.getSourceTask();

        TopoMasterCtrlEvent event = (TopoMasterCtrlEvent) tuple.getValues().get(0);
        long batchId = -1;
        if (event.getEventValue() != null && event.getEventValue().size() > 0) {
            batchId = (long) event.getEventValue().get(0);
        }
        LOG.debug("Received control event from task-{}, event={}", taskId, event);

        boolean isFinished = false;
        switch (event.getEventType()) {
        case transactionInitState:
            if (snapshotState.isRunning()) {
                // If init happened during topology is running, trigger rollback immediately.
                LOG.info("Found task-{} restart request during topology committing. Current state is {}", taskId, snapshotState.getState());
                if (!snapshotState.getState().equals(State.ROLLBACK))
                    stopAndRollback(snapshotState.rollback());
            } else {
                TransactionState state = snapshotState.getInitState(taskId);
                if (state != null) {
                    TopoMasterCtrlEvent initStateResp = new TopoMasterCtrlEvent(EventType.transactionInitState);
                    initStateResp.addEventValue(state);
                    ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(
                            initStateResp));
                } else {
                    LOG.warn("Can not find init state for task-{}", taskId);
                }
            }
            break;
        case transactionCommit:
            TransactionState commitState = (TransactionState) event.getEventValue().get(1);
            isFinished = snapshotState.commit(batchId, taskId, commitState);
            break;
        case transactionAck:
            isFinished = snapshotState.ackEndBolt(batchId, taskId);
            break;
        case transactionRollback:
            // If receiving rollback request, TM will rollback all tasks which are on the same stream as this task
            LOG.debug("Received rollback request. snapshotState: {}", snapshotState);
            if (!snapshotState.getState().equals(State.ROLLBACK))
                stopAndRollback(snapshotState.rollback());
            break;
        default:
            LOG.warn("unexpected event type from task-{}, event={}", taskId, event.toString());
            break;
        }

        if (isFinished) {
            finishSnapshotStateCommit(batchId, snapshotState);
        }
        LOG.debug("snapshotState: {}", snapshotState);
    }

    private void finishSnapshotStateCommit(long batchId, SnapshotState snapshotState) {
        Set<Integer> statefulTasks = new HashSet<>();
        statefulTasks.addAll(snapshotState.getSpoutTasks());
        TopoMasterCtrlEvent resp = null;
        boolean isCommitSuccess = false;
        if (batchId != TransactionCommon.INIT_BATCH_ID) {
            if (snapshotState.isActive()) {
                resp = new TopoMasterCtrlEvent(EventType.transactionCommit);
                resp.addEventValue(batchId);
                resp.addEventValue(System.currentTimeMillis());
                statefulTasks.addAll(snapshotState.getStatefulTasks());

                snapshotState.successBatch(batchId);
                // Try to persist the topology snapshot state. But if any failure happened, just rollback.
                isCommitSuccess = stateOperator.commitState(topologyName, snapshotState);
                if (!isCommitSuccess) {
                    LOG.warn("Failed to commit topology state for batch-{}", batchId);
                    stopAndRollback(snapshotState.rollback());
                }
            } else {
                LOG.info("Ignore finish request for batchId-{}, since current status({}) is not active", batchId, snapshotState.getState());
            }
        } else {
            snapshotState.setActive();
            resp = new TopoMasterCtrlEvent(EventType.transactionStart);
            snapshotState.successBatch(batchId);
            isCommitSuccess = true;
        }

        if (isCommitSuccess) {
            // Ack the init or commit request from spout and stateful bolt
            LOG.debug("Send ack to spouts/statefulBolts-{}, event={}", statefulTasks, resp);
            for (Integer task : statefulTasks) {
                ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(task, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(resp));
            }

            long nextPendingSuccessBatch = snapshotState.getPendingSuccessBatch();
            if (nextPendingSuccessBatch != -1) {
                LOG.info("Try to commit a pending successful batch-{}", nextPendingSuccessBatch);
                finishSnapshotStateCommit(nextPendingSuccessBatch, snapshotState);
            }
        }
    }

    public void expiredCheck() {
        try {
            lock.lock();
            final Map<Integer, TransactionState> tasksToStates = snapshotState.expiredCheck();
            if (tasksToStates.size() > 0) {
                stopAndRollback(tasksToStates);
            }
        } finally {
            lock.unlock();
        }
    }

    private void stopAndRollback(final Map<Integer, TransactionState> tasksToStates) {
        final long lastSuccessBatchId = snapshotState.getLastSuccessfulBatchId();
        for (Entry<String, Set<Integer>> entry : spouts.entrySet()) {
            for (int taskId : entry.getValue()) {
                TopoMasterCtrlEvent stop = new TopoMasterCtrlEvent(EventType.transactionStop);
                ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(stop));
            }

        }
        LOG.info("Stop spouts={}", spouts.keySet());

        scheduledService.schedule(new Runnable() {
            @Override
            public void run() {
                sendRollbackRequest(lastSuccessBatchId, tasksToStates);
            }
        }, 10, TimeUnit.SECONDS);
    }

    private void sendRollbackRequest(long lastSuccessBatchId, Map<Integer, TransactionState> tasksToStates) {
        // Rollback for stateful tasks
        Iterator<Integer> iter = tasksToStates.keySet().iterator();
        while (iter.hasNext()) {
            int taskId = iter.next();
            TransactionState state = tasksToStates.get(taskId) != null ? tasksToStates.get(taskId) : new TransactionState(0, null, null);
            TopoMasterCtrlEvent rollbackRequest = new TopoMasterCtrlEvent(EventType.transactionRollback);
            rollbackRequest.addEventValue(state);
            ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null,
                    new Values(rollbackRequest));
        }
        LOG.info("Send rollback request to tasks:{}", tasksToStates.keySet());

        // Rollback for non-stateful tasks
        TransactionState state = new TransactionState(lastSuccessBatchId, null, null);
        TopoMasterCtrlEvent rollbackRequest = new TopoMasterCtrlEvent(EventType.transactionRollback);
        rollbackRequest.addEventValue(state);
        Set<String> nonStatefulBoltIds = nonStatefulBoltTasks.keySet();
        for (Integer nonStatefulTaskId : context.getComponentsTasks(nonStatefulBoltIds)) {
            ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(nonStatefulTaskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(
                    rollbackRequest));
        }
    }

    public void close() {
        if (stateOperator instanceof Closeable)
            try {
                ((Closeable) stateOperator).close();
            } catch (IOException e) {
                LOG.warn("Failed to close state operator", e);
            }
    }
}