package com.alibaba.jstorm.transactional.state;

import static org.slf4j.LoggerFactory.getLogger;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.execute.BoltCollector;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.utils.JStormUtils;

public class SnapshotStateMaster {
    private static final Logger LOG = getLogger(SnapshotStateMaster.class);

    private String topologyId;
    private String topologyName;
    private StormTopology topology;
    private Map conf;
    private OutputCollector outputCollector;
    private TopologyContext context;

    private Map<String, Integer> groupIds;
    private Map<Integer, List<String>> groupIdToNames;

    private ITopologyStateOperator stateOperator;
    private Map<Integer, SnapshotState> topologySnapshotState;

    private int batchSnapshotTimeout;
    private ScheduledExecutorService scheduledService = null;

    public SnapshotStateMaster(TopologyContext context, OutputCollector outputCollector){
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
        stateOperator.init(conf);
 
        topologySnapshotState = new HashMap<Integer, SnapshotState>();
        Set<String> spoutNames = topology.get_spouts().keySet();
        groupIds = TransactionCommon.groupIds(spoutNames);
        groupIdToNames = JStormUtils.reverse_map(groupIds);
        Set<String> statefulBolts = TransactionCommon.getStatefulBolts(topology);
        for (Integer groupId : groupIdToNames.keySet()) {
            Set<Integer> spoutTasks = new HashSet<Integer>();
            Set<Integer> statefulBoltTasks = new HashSet<Integer>();
            Set<Integer> nonStatefulBoltTasks = new HashSet<Integer>();
            Set<Integer> endBoltTasks = new HashSet<>();
            for (String spoutName : spoutNames) {
                // Get spout tasks
                spoutTasks.addAll(context.getComponentTasks(spoutName));
                // Get downstream stateful bolt tasks
                Set<String> downstreamComponents = TransactionCommon.getAllDownstreamComponents(spoutName, topology);
                Set<String> statefulDownstreamComponents = new HashSet<String>(downstreamComponents);
                statefulDownstreamComponents.retainAll(statefulBolts);
                statefulBoltTasks.addAll(context.getComponentsTasks(statefulDownstreamComponents));
                // Get downstream non-stateful bolt tasks
                downstreamComponents.removeAll(statefulDownstreamComponents);
                nonStatefulBoltTasks.addAll(context.getComponentsTasks(downstreamComponents));
                // Get end bolt tasks
                Set<String> endBolts = TransactionCommon.getEndBolts(topology, spoutName);
                endBoltTasks.addAll(context.getComponentsTasks(endBolts));  
            }
            topologySnapshotState.put(groupId, new SnapshotState(groupId, spoutTasks, statefulBoltTasks, nonStatefulBoltTasks, endBoltTasks));
        }

        HashMap<Integer, SnapshotState> topologyCommitState = (HashMap<Integer, SnapshotState>) stateOperator.initState(topologyName);
        if (topologyCommitState != null && topologyCommitState.size() > 0) {
            for (Entry<Integer, SnapshotState> entry : topologyCommitState.entrySet()) {
                int id = entry.getKey();
                SnapshotState commitState = entry.getValue();
                topologySnapshotState.get(id).setLastSuccessfulBatch(commitState);
                topologySnapshotState.get(id).setState(commitState.getState());
            }
        }
        LOG.info("topologySnapshotState=" + topologySnapshotState);

        this.batchSnapshotTimeout = ConfigExtension.getTransactionBatchSnapshotTimeout(conf);
        scheduledService = Executors.newSingleThreadScheduledExecutor();
        scheduledService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                expiredCheck();
            } 
        }, batchSnapshotTimeout, batchSnapshotTimeout / 2, TimeUnit.SECONDS);
    }

    public synchronized void process(Tuple tuple) {
        int taskId = tuple.getSourceTask();

        TopoMasterCtrlEvent event = (TopoMasterCtrlEvent) tuple.getValues().get(0);
        BatchGroupId batchGroupId = null;
        if (event.getEventValue() != null && event.getEventValue().size() > 0) {
            batchGroupId =  (BatchGroupId) event.getEventValue().get(0);
        }
        SnapshotState snapshotState = null;
        // If batchGroupId == null, it is initState request
        if (batchGroupId != null) {
            snapshotState = topologySnapshotState.get(batchGroupId.groupId);
            if (snapshotState == null) {
                LOG.warn("unexpected event from task-{}, event={}", taskId, event.toString());;
                return;
            }
        }
        LOG.debug("Received control event from task-{}, event={}", taskId, event);

        boolean isFinished = false;
        switch(event.getEventType()) {
            case transactionInitState:
                for (Entry<Integer, SnapshotState> entry : topologySnapshotState.entrySet()) {
                    TransactionState state = entry.getValue().getInitState(taskId);
                    int groupId = entry.getKey();
                    if (state != null) {
                        TopoMasterCtrlEvent initStateResp = new TopoMasterCtrlEvent(EventType.transactionInitState);
                        initStateResp.addEventValue(state);
                        ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, 
                                new Values(initStateResp));
                    }
                }
                break;
            case transactionCommit:
                TransactionState commitState = (TransactionState) event.getEventValue().get(1);
                isFinished = snapshotState.commit(batchGroupId.batchId, taskId, commitState);
                break;
            case transactionAck:
                isFinished = snapshotState.ackEndBolt(batchGroupId.batchId, taskId);
                break;
            case transactionRollback:
                // If receiving rollback request, TM will rollback all tasks which are on the same stream as this task
                stopAndRollback(batchGroupId.groupId, snapshotState.rollback());
                break;
            default:
                LOG.warn("unexpected event type from task-{}, event={}", taskId, event.toString());
                break;
        }

        if(isFinished) {
            finishSnapshotStateCommit(batchGroupId, snapshotState);
        }
        LOG.debug("snapshotState: {}", snapshotState);
    }

    private void finishSnapshotStateCommit(BatchGroupId batchGroupId, SnapshotState snapshotState) {
        Set<Integer> statefulTasks = new HashSet<Integer>();
        statefulTasks.addAll(snapshotState.getSpoutTasks());
        TopoMasterCtrlEvent resp = null;
        boolean isCommitSuccess = false;
        if (batchGroupId.batchId != TransactionCommon.INIT_BATCH_ID) {
            if (snapshotState.isActive()) {
                resp = new TopoMasterCtrlEvent(EventType.transactionCommit);
                resp.addEventValue(batchGroupId);
                statefulTasks.addAll(snapshotState.getStatefulTasks());

                snapshotState.successBatch(batchGroupId.batchId);
                // Try to persist the topology snapshot state. But if any failure happened, just continue.
                try {
                    isCommitSuccess = stateOperator.commitState(topologyName, batchGroupId.groupId, topologySnapshotState);
                    if(!isCommitSuccess) {
                        LOG.warn("Failed to commit topology state for batch-{}", batchGroupId);
                    }
                } catch (Exception e) {
                    LOG.warn("Got exception, when committing topology state for batch-{}, {}", batchGroupId, e);
                }
            }
        } else {
            snapshotState.setActive();
            resp = new TopoMasterCtrlEvent(EventType.transactionStart);
            snapshotState.successBatch(batchGroupId.batchId);
            isCommitSuccess = true;
        }

        if (isCommitSuccess) {
            // Ack the init or commit request from spout and stateful bolt
            LOG.debug("Send ack to spouts-{}, event={}", statefulTasks, resp);
            for (Integer spoutTask : statefulTasks) {
                ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(spoutTask, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, 
                        new Values(resp));
            }
        }

        long nextPendingSuccessBatch = snapshotState.getPendingSuccessBatch();
        if (nextPendingSuccessBatch != -1) {
            LOG.info("Try to commit a pending successful batch-{}", nextPendingSuccessBatch);
            finishSnapshotStateCommit(new BatchGroupId(batchGroupId.groupId, nextPendingSuccessBatch), snapshotState);
        }
    }

    public void expiredCheck() {
        for (Entry<Integer, SnapshotState> entry : topologySnapshotState.entrySet()) {
            final Map<Integer, TransactionState> tasksToStates = entry.getValue().expiredCheck();
            if (tasksToStates.size() > 0) {
                stopAndRollback(entry.getKey(), tasksToStates);
            }
        }
    }

    private void stopAndRollback(final int groupId, final Map<Integer, TransactionState> tasksToStates) {
        List<String> spoutNames = groupIdToNames.get(groupId);
        for (String spoutName : spoutNames) {
            for (int taskId : context.getComponentTasks(spoutName)) {
                TopoMasterCtrlEvent stop = new TopoMasterCtrlEvent(EventType.transactionStop);
                stop.addEventValue(groupId);
                ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, 
                    new Values(stop));
            }
        }
        LOG.info("Stop spouts={}, tasks={}", spoutNames, context.getComponentsTasks(new HashSet<String>(spoutNames)));

        scheduledService.schedule(new Runnable() {
            @Override
            public void run() {
                sendRollbackRequest(groupId, tasksToStates);
            }
        }, 10, TimeUnit.SECONDS);
    }

    private void sendRollbackRequest(int groupId, Map<Integer, TransactionState> tasksToStates) {
        // Rollback for stateful tasks
        Iterator<Integer> iter = tasksToStates.keySet().iterator();
        while (iter.hasNext()) {
            int taskId = iter.next();
            TransactionState state = tasksToStates.get(taskId) != null ? tasksToStates.get(taskId) : new TransactionState(groupId, 0, null, null);
            TopoMasterCtrlEvent rollbackRequest = new TopoMasterCtrlEvent(EventType.transactionRollback);
            rollbackRequest.addEventValue(state);
            ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, 
                    new Values(rollbackRequest));
        }
        LOG.info("Send rollback request to group:{}, tasks:{}", groupIdToNames.get(groupId), tasksToStates.keySet());

        // Rollback for non-stateful tasks
        SnapshotState snapshot = topologySnapshotState.get(groupId);
        TransactionState state = new TransactionState(groupId, snapshot.getLastSuccessfulBatchId(), null, null);
        TopoMasterCtrlEvent rollbackRequest = new TopoMasterCtrlEvent(EventType.transactionRollback);
        rollbackRequest.addEventValue(state);
        for (Integer nonStatefulTaskId : snapshot.getNonStatefulTasks()) {
            ((BoltCollector) (outputCollector.getDelegate())).emitDirectCtrl(nonStatefulTaskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, 
                    new Values(rollbackRequest));
        }
    }
}