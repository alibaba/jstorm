package com.alibaba.jstorm.transactional.state;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Set;

import org.slf4j.Logger;

import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.utils.RotatingMap;

public class SnapshotState implements Serializable {
    private static final long serialVersionUID = -4997799343186429338L;

    private static final Logger LOG = getLogger(SnapshotState.class);

    private int groupId;

    private transient Set<Integer> sourceTasks;
    private transient Set<Integer> statefulTasks;
    private transient Set<Integer> nonStatefulTasks;
    private transient Set<Integer> endTasks;

    private BatchStateTracker lastSuccessfulSnapshot;
    private transient RotatingMap<Long, BatchStateTracker> inprogressSnapshots;

    private State state;

    public class BatchStateTracker implements Serializable {
        private static final long serialVersionUID = -3503873401193374360L;

        private long batchId;
        private Map<Integer, TransactionState> spouts = new HashMap<Integer, TransactionState>();
        private Map<Integer, TransactionState> statefulBolts = new HashMap<Integer, TransactionState>();
        private transient Map<Integer, Boolean> endBolts = new HashMap<Integer, Boolean>();
        private transient int receivedSpoutMsgCount = 0;
        private transient int receivedStatefulBoltMsgCount = 0;
        private transient int receivedEndBoltMsgCount = 0;

        public BatchStateTracker(long batchId, Set<Integer> spouts, Set<Integer> statefulBolts, Set<Integer> endBolts) {
            this.batchId = batchId;

            for (Integer taskId : spouts) {
                this.spouts.put(taskId, null);
            }
            for (Integer taskId : statefulBolts) {
                this.statefulBolts.put(taskId, null);
            }
            for (Integer taskId : endBolts) {
                this.endBolts.put(taskId, false);
            }
        }

        public long getBatchId() {
            return batchId;
        }

        public Map<Integer, TransactionState> getSpoutStates() {
            return spouts;
        }

        public Map<Integer, TransactionState> getStatefulBoltStates() {
            return statefulBolts;
        }

        public void updateSpout(int taskId, TransactionState state) {
            if (spouts.containsKey(taskId) == false) {
                LOG.warn("Received unexpected task-{} when updating spout", taskId);
                return;
            }

            if (spouts.put(taskId, state) != null) {
                LOG.warn("Duplicated state commit for spout-{}, state={}", taskId, state);
            } else {
                receivedSpoutMsgCount++;
            }
        }

        public void updateStatefulBolt(int taskId, TransactionState state) {
            if (statefulBolts.containsKey(taskId) == false) {
                LOG.warn("Received unexpected task-{} when updating statefulBolts", taskId);
                return;
            }

            if (statefulBolts.put(taskId, state) != null) {
                LOG.warn("Duplicated state commit for statefulBolt-{}, state={}", taskId, state);
            } else {
                receivedStatefulBoltMsgCount++;
            }
        }

        public void updateEndBolt(int taskId) {
            if (endBolts.containsKey(taskId) == false) {
                LOG.warn("Received unexpected task-{} when updating endBolts", taskId);
                return;
            }

            if (endBolts.put(taskId, true) != false) {
                LOG.warn("Duplicated ack for endBolt-{}", taskId);
            } else {
                receivedEndBoltMsgCount++;
            }
        }

        public boolean isFinished() {
            if (state.equals(State.ACTIVE) && batchId != TransactionCommon.INIT_BATCH_ID) {
                if (receivedSpoutMsgCount == spouts.size() && receivedStatefulBoltMsgCount == statefulBolts.size() &&
                        receivedEndBoltMsgCount == endBolts.size()) {
                    /*long expectedNextBatchId = getNextExpectedSuccessfulBatch();
                    if (batchId == expectedNextBatchId) {
                        return true;
                    } else {
                        LOG.info("Unexpected forward batch-{} was finished. But the expected is batch-{}", batchId, expectedNextBatchId);
                        return false;
                    }*/
                    return true;
                } else {
                    return false;
                }
            } else {
                // restart is done for rollback or initState
                return receivedEndBoltMsgCount == endBolts.size();
            }
        }

        @Override
        public String toString() {
            return "batchId=" + batchId + ", receivedSpoutMsgCount=" + receivedSpoutMsgCount + ", receivedStatefulBoltMsgCount=" + 
                    receivedStatefulBoltMsgCount + ", receivedEndBoltMsgCount=" + receivedEndBoltMsgCount;
        }

        public String hasNotCommittedTasksInfo() {
        	Set<Integer> notCommitSpouts = new HashSet<Integer>();
        	for (Entry<Integer, TransactionState> entry : spouts.entrySet()) {
        		if (entry.getValue() == null) {
        			notCommitSpouts.add(entry.getKey());
        		}
        	}

        	Set<Integer> notCommitBolts = new HashSet<Integer>();
        	for (Entry<Integer, TransactionState> entry : statefulBolts.entrySet()) {
        		if (entry.getValue() == null) {
        			notCommitBolts.add(entry.getKey());
        		}
        	}

        	Set<Integer> notCommitEndBolts = new HashSet<Integer>();
        	for (Entry<Integer, Boolean> entry : endBolts.entrySet()) {
        		if (!entry.getValue()) {
        			notCommitEndBolts.add(entry.getKey());
        		}
        	}
        	return "Not Committed tasks: Spouts=" + notCommitSpouts + ", Bolts=" + notCommitBolts + ", EndBolts=" + notCommitEndBolts;
        }
    }

    public SnapshotState(int groupId, Set<Integer> spouts, Set<Integer> statefulBolts, Set<Integer> nonStatefulBolts, Set<Integer> endBolts) {
        this.groupId = groupId;
        this.sourceTasks = spouts;
        this.statefulTasks = statefulBolts;
        this.nonStatefulTasks = nonStatefulBolts;
        this.endTasks = endBolts;

        this.lastSuccessfulSnapshot  = new BatchStateTracker(TransactionCommon.INIT_BATCH_ID, spouts, statefulBolts, nonStatefulBolts);
        this.inprogressSnapshots = new RotatingMap<Long, BatchStateTracker>(3, true);

        this.state = State.ACTIVE;
    }

    

    private BatchStateTracker getStateTracker(long batchId) {
        if (batchId != TransactionCommon.INIT_BATCH_ID) {
            // If expired batch, just return null
            if (batchId <= lastSuccessfulSnapshot.getBatchId()) {
                LOG.warn("Received expired event for batchId-{}", batchId);
                LOG.warn("Current inprogress snapshots: {}", inprogressSnapshots);
                // In case there is still batch info in inprogress snapshots 
                inprogressSnapshots.remove(batchId);
                return null;
            }
        }

        BatchStateTracker stateTracker = inprogressSnapshots.get(batchId);
        if (stateTracker == null) {
            stateTracker = new BatchStateTracker(batchId, sourceTasks, statefulTasks, endTasks);
            inprogressSnapshots.put(batchId, stateTracker);
        }
        return stateTracker;
    }

    /**
     * 
     * @param batchId
     * @param taskId
     * @param state
     * @return true if current batch is done
     */
    public boolean commit(long batchId, int taskId, TransactionState state) {
        if (isActive()) {
            if (sourceTasks.contains(taskId)) {
                return commitSource(batchId, taskId, state);
            } else if (statefulTasks.contains(taskId)) {
                return commitStatefulBolt(batchId, taskId, state);
            }
        }
        return false;
    }

    /**
     * 
     * @param batchId
     * @param taskId
     * @param state
     * @return true if current batch is done
     */
    private boolean commitSource(long batchId, int taskId, TransactionState state) {
        BatchStateTracker stateTracker = getStateTracker(batchId);
        if (stateTracker != null) {
            stateTracker.updateSpout(taskId, state);
            return stateTracker.isFinished();
        } else {
            return false;
        }
    }

    /**
     * 
     * @param batchId
     * @param taskId
     * @param state
     * @return true if current batch is done
     */
    private boolean commitStatefulBolt(long batchId, int taskId, TransactionState state) {
        BatchStateTracker stateTracker = getStateTracker(batchId);
        if (stateTracker != null) {
            stateTracker.updateStatefulBolt(taskId, state);
            return stateTracker.isFinished();
        } else {
            return false;
        }
    }

    /**
     * 
     * @param batchId
     * @param taskId
     * @return true if current batch is done
     */
    public boolean ackEndBolt(long batchId, int taskId) {
        BatchStateTracker stateTracker = getStateTracker(batchId);
        if (stateTracker != null) {
            stateTracker.updateEndBolt(taskId);
            return stateTracker.isFinished();
        } else {
            return false;
        }
    }

    /**
     * 
     * @return last successful snapshot
     */
    public Map<Integer, TransactionState> rollback() {
        inprogressSnapshots.clear();
        state = State.ROLLBACK;
        getStateTracker(TransactionCommon.INIT_BATCH_ID);
        Map<Integer, TransactionState> snapshotState = new HashMap<Integer, TransactionState>();
        if (lastSuccessfulSnapshot.getBatchId() != TransactionCommon.INIT_BATCH_ID) {
            snapshotState.putAll(lastSuccessfulSnapshot.getSpoutStates());
            snapshotState.putAll(lastSuccessfulSnapshot.getStatefulBoltStates());
        } else {
        	for (Integer spoutId : sourceTasks) {
        		snapshotState.put(spoutId, null);
        	}
        	for (Integer statefulBoltId : statefulTasks) {
        		snapshotState.put(statefulBoltId, null);
        	}
        }
        return snapshotState;
    }

    public TransactionState getInitState(int taskId) {
        TransactionState state = null;
        if (sourceTasks.contains(taskId)) {
            if (lastSuccessfulSnapshot.getBatchId() != TransactionCommon.INIT_BATCH_ID) {
                state = lastSuccessfulSnapshot.getSpoutStates().get(taskId);
            } else {
                state = new TransactionState(groupId, TransactionCommon.INIT_BATCH_ID, null, null);
            }
        } else if (statefulTasks.contains(taskId)) {
            if (lastSuccessfulSnapshot.getBatchId() != TransactionCommon.INIT_BATCH_ID) {
                state = lastSuccessfulSnapshot.getStatefulBoltStates().get(taskId);
            } else {
                state = new TransactionState(groupId, TransactionCommon.INIT_BATCH_ID, null, null);
            }
        } else if (nonStatefulTasks.contains(taskId)) {
            state = new TransactionState(groupId, lastSuccessfulSnapshot.batchId, null, null);
        }
        
        return state;
    }

    public void successBatch(long batchId) {
        if (batchId == TransactionCommon.INIT_BATCH_ID) {
            inprogressSnapshots.remove(batchId);
        } else {
            long id = lastSuccessfulSnapshot.getBatchId() + 1;
            for (; id < batchId; id++) {
                inprogressSnapshots.remove(id);
            }
            lastSuccessfulSnapshot = (BatchStateTracker) inprogressSnapshots.remove(batchId);
        }
    }

    private long getNextExpectedSuccessfulBatch() {
        return lastSuccessfulSnapshot.getBatchId() + 1;
    }

    /**
     * 
     * @return -1: none pending successful batch
     *         >1: batch id of next pending successful batch
     */
    public long getPendingSuccessBatch() {
        long nextBatchId = getNextExpectedSuccessfulBatch();
        BatchStateTracker nextBatch = inprogressSnapshots.get(nextBatchId);
        if (nextBatch == null) {
            return -1;
        } else {
            if(nextBatch.isFinished()) {
                return nextBatch.batchId;
            } else {
                return -1;
            }
        }
    }

    public Set<Integer> getSpoutTasks() {
        return sourceTasks;
    }

    public Set<Integer> getStatefulTasks() {
        return statefulTasks;
    }

    /**
     * 
     * @return Map<taskId, LastSuccessfulTransactionState>
     */
    public Map<Integer, TransactionState> expiredCheck() {
        Map<Integer, TransactionState> rollbackSnapshots = new HashMap<Integer, TransactionState>();
        Map<Long, BatchStateTracker> expiredSnapshots = inprogressSnapshots.rotate();
        if (expiredSnapshots.size() > 0) {
            LOG.info("Found expired batch for group: {}", groupId);
            for (BatchStateTracker tracker : expiredSnapshots.values()) {
                LOG.info("{}, {}", tracker, tracker.hasNotCommittedTasksInfo());
            }
            rollbackSnapshots = rollback();
        }
        return rollbackSnapshots;
    }

    public boolean isActive() {
        return state.equals(State.ACTIVE);
    }

    public void setActive() {
        state = State.ACTIVE;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public BatchStateTracker getLastSuccessfulBatch() {
        return lastSuccessfulSnapshot;
    }

    public void setLastSuccessfulBatch(SnapshotState snapshotState) {
        this.lastSuccessfulSnapshot = snapshotState.getLastSuccessfulBatch();
    }

    public long getLastSuccessfulBatchId() {
        return lastSuccessfulSnapshot.getBatchId();
    }

    public Set<Integer> getNonStatefulTasks() {
        return nonStatefulTasks;
    }

    @Override
    public String toString() {
        return "state=" + state.toString() + ", sourceTasks=" + sourceTasks + ", statefulTasks=" + statefulTasks + ", nonStatefulTasks" + nonStatefulTasks + 
                ", endTasks=" + endTasks + ", inprogressSnapshots" + inprogressSnapshots + ", lastSuccessfulSnapshot=" + lastSuccessfulSnapshot;
    }
}