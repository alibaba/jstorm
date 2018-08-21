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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionStatefulBolt extends TransactionBolt {
    private static final long serialVersionUID = 6590819896204525523L;

    private static Logger LOG = LoggerFactory.getLogger(TransactionStatefulBolt.class);

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
            long batchId = -1;
            Pair<EventType, Object> commitEvent = null;
            BatchTracker commitBatch = null;
            while (true) {
                try {
                    if (boltStatus.equals(State.ACTIVE) && isActive) {
                        commitEvent = committingQueue.take();
                        switch (commitEvent.getFirst()) {
                            case transactionCommit:
                                commitBatch = (BatchTracker) commitEvent.getSecond();
                                batchId = commitBatch.getBatchId();
                                if (commitBatch != null) {
                                    persistCommit(commitBatch);
                                    commitBatch = null;
                                }
                                break;
                            case transactionAck:
                                List<Object> value = (List<Object>) commitEvent.getSecond();
                                batchId = (long) value.get(0);
                                boltExecutor.ackCommit(batchId, (long) value.get(1));
                                break;
                            default:
                                LOG.warn("Unexpected commit event: {}", commitEvent.getFirst());
                                break;
                        }
                    } else {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    if (isActive) {
                        LOG.info("Aysnc thread was interrupted. Current committing batch: {}", commitBatch);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to process event=" + commitEvent + " for batch-" + batchId, e);
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
        this.committingQueue = new LinkedBlockingDeque<>(50);
        startAsyncCommitThread();
    }

    @Override
    protected void initState(TransactionState state) {
        super.initState(state);
        boltExecutor.initState(state.getUserCheckpoint());
    }

    @Override
    protected void commit() {
        BatchTracker committingBatch = currentBatchTracker;
        committingBatch.getState().setUserCheckpoint(Utils.trySerialize(boltExecutor.finishBatch(currentBatchTracker.getBatchId())));
        try {
            committingQueue.put(new Pair<EventType, Object>(EventType.transactionCommit, committingBatch));
        } catch (InterruptedException e) {
            LOG.error("Error when committing for batch " + committingBatch, e);
            fail(committingBatch.getBatchId());
        }
    }

    @Override
    protected void ackCommit(List<Object> value) {
        try {
            committingQueue.put(new Pair<EventType, Object>(EventType.transactionAck, value));
        } catch (InterruptedException e) {
            LOG.warn("Failed to publish ack commit for batch-{}", (long) value.get(0));
        }
    }

    @Override
    protected void rollback(TransactionState state) {
        LOG.info("Start to rollback, state={}\n  currentBatchStatus: {}", state, currentBatchStatusInfo());
        boltExecutor.rollBack(state.getUserCheckpoint());
        long batchId = state.getCurrBatchId();
        lastSuccessfulBatch = batchId;
    }

    @Override
    protected void cleanupBuffer() {
        super.cleanupBuffer();
        cleanupCommittingJobs();
    }

    private void startAsyncCommitThread() {
        asyncCommittingThread = new AsyncCommittingThread(new AsyncCommitRunnable());
        asyncCommittingThread.setName("AsyncCommitThread-task-" + taskId);
        asyncCommittingThread.start();
    }

    protected void persistCommit(BatchTracker committingBatch) {
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionCommit);
        event.addEventValue(committingBatch.getBatchId());

        byte[] persistData = (byte[]) committingBatch.getState().getUserCheckpoint();
        Object persistKey;
        try {
            persistKey = boltExecutor.commit(committingBatch.getBatchId(), Utils.maybe_deserialize(persistData));
        } catch (Exception e) {
            LOG.warn("Failed to persist user checkpoint for batch-" + committingBatch.getBatchId(), e);
            fail(committingBatch.getBatchId());
            return;
        }
        if (persistKey == TransactionCommon.COMMIT_FAIL) {
            LOG.warn("Failed to persist user checkpoint for batch-{}", committingBatch.getBatchId());
            fail(committingBatch.getBatchId());
            return;
        }

        committingBatch.getState().setUserCheckpoint(persistKey);
        event.addEventValue(committingBatch.getState());
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    private void cleanupCommittingJobs() {
        asyncCommittingThread.deactivate();
        asyncCommittingThread.interrupt();
        committingQueue.clear();
        asyncCommittingThread.activate();
    }
}