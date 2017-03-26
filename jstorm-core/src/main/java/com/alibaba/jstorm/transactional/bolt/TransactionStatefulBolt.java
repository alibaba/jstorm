package com.alibaba.jstorm.transactional.bolt;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;

public class TransactionStatefulBolt extends TransactionBolt {
    private static final long serialVersionUID = 6590819896204525523L;

    private ITransactionStatefulBoltExecutor boltExecutor;

    // async jobs to do commit
    private BlockingQueue<Pair<EventType, Object>> committingQueue;
    private AsyncCommittingThread asyncCommittingThread;

    private class AsyncCommittingThread extends Thread {
        AsyncCommitRunnable runnable;

        public AsyncCommittingThread(AsyncCommitRunnable runnable) {
            super(runnable);
            this.runnable = runnable;
        }

        public void deactivate() {
            runnable.isActive = false;
        }

        public void activate() {
            runnable.isActive = true;
        }
    }

    private class AsyncCommitRunnable implements Runnable {
        public boolean isActive = true;

        @Override
        public void run() {
            BatchTracker commitBatch = null;
            while(true) {
                try {
                    if (boltStatus.equals(State.ACTIVE) && isActive) {   
                        Pair<EventType, Object> commitEvent = committingQueue.take();
                        switch(commitEvent.getFirst()) {
                        case transactionCommit:
                            commitBatch = (BatchTracker) commitEvent.getSecond();
                            if (commitBatch != null) {
                                persistCommit(commitBatch);
                                commitBatch = null;
                            }
                            break;
                        case transactionAck:
                            boltExecutor.ackCommit((BatchGroupId) commitEvent.getSecond());
                            break;
                        default:
                            LOG.warn("Unexpected commit event: {}", commitEvent.getFirst());
                            break;
                        }
                    } else {
                        Thread.sleep(5);
                    }
                } catch (InterruptedException e) {
                    if (isActive) {
                        LOG.info("Aysnc thread was interrupted. Current committingbatch: {}", commitBatch);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to ack commit for batch: {}", commitBatch, e);
                }
            }
        }
    }

    public TransactionStatefulBolt(ITransactionStatefulBoltExecutor boltExecutor) {
        super(boltExecutor);
        this.boltExecutor = boltExecutor;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.committingQueue = new LinkedBlockingDeque<Pair<EventType, Object>>(50);
        startAsyncCommitThread(); 
    }

    @Override
    protected void initState(TransactionState state) {
        super.initState(state);
        Object userState = Utils.maybe_deserialize((byte[]) state.getUserCheckpoint());
        boltExecutor.initState(userState);
    }

    @Override
    protected void commit() {
        BatchTracker committingBatch = currentBatchTracker;
        committingBatch.getState().setUserCheckpoint(Utils.trySerialize(boltExecutor.finishBatch()));
        try {
            committingQueue.put(new Pair<EventType, Object>(EventType.transactionCommit, committingBatch));
        } catch (InterruptedException e) {
            LOG.error("Error when committing for batch {}", committingBatch, e);
            fail(committingBatch.getBatchGroupId());
        }
    }

    @Override
    protected void ackCommit(BatchGroupId batchGroupId) {
        try {
            committingQueue.put(new Pair<EventType, Object>(EventType.transactionAck, batchGroupId));
        } catch (InterruptedException e) {
            LOG.warn("Failed to publish ack commit for batch {}", batchGroupId);
        }
    }

    @Override
    protected void rollback(TransactionState state) {
        LOG.info("Start to rollback, state={}, currentBatchStatus={}", state, currentBatchStatusInfo());
        boltExecutor.rollBack(Utils.maybe_deserialize((byte[]) state.getUserCheckpoint()));
        BatchGroupId batchGroupId = state.getCurrBatchGroupId();
        lastSuccessfulBatch.put(batchGroupId.groupId, batchGroupId.batchId);
        cleanupBuffer(state.getCurrBatchGroupId().groupId);
    }

    @Override
    protected void cleanupBuffer(int groupId) {
        super.cleanupBuffer(groupId);
        cleanupCommittingJobs();
    }

    private void startAsyncCommitThread() {
        asyncCommittingThread = new AsyncCommittingThread(new AsyncCommitRunnable());
        asyncCommittingThread.setName("AsyncCommitThread-task-" + taskId);
        asyncCommittingThread.start();
    }

    protected void persistCommit(BatchTracker committingBatch) {
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionCommit);
        event.addEventValue(committingBatch.getBatchGroupId());

        byte[] persistData = (byte[]) committingBatch.getState().getUserCheckpoint();
        Object persistKey = null;
        try {
            persistKey = boltExecutor.commit(committingBatch.getBatchGroupId(), Utils.maybe_deserialize(persistData));
        } catch (Exception e) {
            LOG.warn("Failed to persist user checkpoint for batch-{}", committingBatch.getBatchGroupId());
            return;
        }
        if (persistKey == TransactionCommon.COMMIT_FAIL) {
            LOG.warn("Failed to persist user checkpoint for batch-{}", committingBatch.getBatchGroupId());
            return;
        }

        committingBatch.getState().setUserCheckpoint(Utils.trySerialize(persistKey));
        event.addEventValue(committingBatch.getState());
        outputCollector.emitDirectByDelegate(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    private void cleanupCommittingJobs() {
        asyncCommittingThread.deactivate();
        asyncCommittingThread.interrupt();
        committingQueue.clear();
        asyncCommittingThread.activate();
    }
}