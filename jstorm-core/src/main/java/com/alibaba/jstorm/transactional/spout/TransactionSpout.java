package com.alibaba.jstorm.transactional.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.ICtrlMsgSpout;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.TransactionOutputFieldsDeclarer;
import com.alibaba.jstorm.transactional.spout.TransactionSpoutOutputCollector.BatchInfo;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

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
    protected Set<Integer> downstreamTasks;

    protected ITransactionSpoutExecutor spoutExecutor;
    protected TransactionSpoutOutputCollector outputCollector;

    protected volatile State spoutStatus;

    protected IntervalCheck initRetryCheck;

    protected int groupId;
    protected TransactionState currState;

    protected SortedSet<Long> committingBatches;
    protected volatile boolean isMaxPending;
    protected int MAX_PENDING_BATCH_NUM;   
    protected int MAX_FAIL_RETRY;

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
        this.groupId = TransactionCommon.groupIndex(context.getRawTopology(), componentId);
        this.downstreamTasks = TransactionCommon.getDownstreamTasks(componentId, topologyContext);

        this.outputCollector = new TransactionSpoutOutputCollector(collector, this);

        this.spoutStatus = State.INIT;
        this.committingBatches = new TreeSet<Long>();
        this.isMaxPending = false;
        this.MAX_PENDING_BATCH_NUM = JStormUtils.parseInt(conf.get("transaction.max.pending.batch"), 2);

        int taskLaunchTimeout = JStormUtils.parseInt(conf.get(Config.NIMBUS_TASK_LAUNCH_SECS));
        int spoutInitRetryDelaySec = JStormUtils.parseInt(conf.get("transaction.spout.init.retry.secs"), taskLaunchTimeout);
        this.initRetryCheck = new IntervalCheck();
        initRetryCheck.setInterval(spoutInitRetryDelaySec);

        this.lock = new ReentrantLock(true);

        spoutExecutor.open(conf, context, new SpoutOutputCollector(outputCollector));
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
        declarer.declareStream(TransactionCommon.BARRIER_STREAM_ID, new Fields(TransactionCommon.BATCH_GROUP_ID_FIELD, 
                TransactionCommon.BARRIER_SNAPSHOT_FIELD));

        TransactionOutputFieldsDeclarer transactionDeclarer = new TransactionOutputFieldsDeclarer();
        spoutExecutor.declareOutputFields(transactionDeclarer);
        Map<String, StreamInfo> streams = transactionDeclarer.getFieldsDeclaration();
        for (Entry<String, StreamInfo> entry : streams.entrySet()) {
            String streamName = entry.getKey();
            StreamInfo streamInfo = entry.getValue();
            declarer.declareStream(streamName, streamInfo.is_direct(), new Fields(streamInfo.get_output_fields()));
        }
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
        //LOG.info("Received contronl event, {}", event.toString());
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
		        rollbackSpoutState(state);
		        LOG.info("Rollback to state, {}", state);
		        JStormUtils.sleepMs(5000);
		        outputCollector.flushInitBarrier();
		        break;
		    case transactionCommit:
		        BatchGroupId successBatchGroupId = (BatchGroupId) event.getEventValue().get(0);
		        removeSuccessBatch(successBatchGroupId.batchId);
		        //LOG.info("Commit Acked, current pending batchs: {}", committingBatches);
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
        return (isMaxPending == false) && spoutStatus.equals(State.ACTIVE);
    }

    protected void startInitState() {
        LOG.info("Start to retrieve the initial state from topology master");
        // retrieve state from topology master
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionInitState);
        outputCollector.emitDirectByDelegate(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(event), null);
    }

    private void initSpoutState(TransactionState state) {
    	LOG.info("Initial state for spout: {}", state);
        if (state == null) {
            currState = new TransactionState(groupId, TransactionCommon.INIT_BATCH_ID, null, null);
        } else {
            currState = new TransactionState(state);
        }
        Object userState = Utils.maybe_deserialize((byte[]) currState.getUserCheckpoint());
        spoutExecutor.initState(userState);
        resetSpoutState();
    }

    private void rollbackSpoutState(TransactionState state) {
        if (state != null) {
            currState = new TransactionState(state);
            Object userState = Utils.maybe_deserialize((byte[]) state.getUserCheckpoint());
            spoutExecutor.rollBack(userState);
        }
        resetSpoutState();
    }

    private void resetSpoutState() {
        committingBatches.clear();
        updateMaxPendingFlag();
        outputCollector.init(currState.getCurrBatchGroupId(), downstreamTasks);
        outputCollector.moveToNextBatch();
    }

    protected void commit() {
        if (isActive() == false) {
            // If spout has not finished initialization or is inactive, just return.
            return;
        }

        Object userState = spoutExecutor.finishBatch();
        BatchInfo batchInfo = outputCollector.flushBarrier();
        BatchGroupId id = new BatchGroupId(groupId, batchInfo.batchId);
        Object commitState = null;
        try {
            commitState = spoutExecutor.commit(id, userState);
        } catch (Exception e) {
            LOG.warn("Failed to commit spout state for batch-{}, {}", id, e);
            return;
        }
        if (commitState == TransactionCommon.COMMIT_FAIL) {
            LOG.warn("Failed to commit spout state for batch-{}", id);
            return;
        }

        currState.setBatchId(batchInfo.batchId);
        currState.setUserCheckpoint(Utils.trySerialize(commitState));
        
        committingBatches.add(batchInfo.batchId);
        updateMaxPendingFlag();

        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionCommit);
        TransactionState state = new TransactionState(currState);
        event.addEventValue(state.getCurrBatchGroupId());
        event.addEventValue(state);
        outputCollector.emitDirectByDelegate(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(event), null);
    }

    private void removeSuccessBatch(long successBatchId) {
        SortedSet<Long> obsoleteBatches = new TreeSet<Long>(committingBatches.headSet(successBatchId));
        if (obsoleteBatches.size() > 0) {
            LOG.info("Obsolete batcheIds are {}, successBatchId is {}", obsoleteBatches, successBatchId);
            committingBatches.removeAll(obsoleteBatches);
        }
        if (committingBatches.remove(successBatchId) == false) {
            LOG.info("Batch-{} has alreay been removed", successBatchId);
        }
        updateMaxPendingFlag();
    }

    private void updateMaxPendingFlag() {
    	if (MAX_PENDING_BATCH_NUM == 0) {
    		isMaxPending = false;
    	} else {
            if (committingBatches.size() < MAX_PENDING_BATCH_NUM) {
                isMaxPending = false;
            } else {
                isMaxPending = true;
            }
    	}
    }
}