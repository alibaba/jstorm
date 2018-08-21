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

import static org.slf4j.LoggerFactory.getLogger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.transactional.state.task.ITaskStateInitOperator;
import com.alibaba.jstorm.utils.RotatingMap;

public class SnapshotState implements Serializable {
    private static final long serialVersionUID = -4997799343186429338L;

    private static final Logger LOG = getLogger(SnapshotState.class);

    private transient TopologyContext context;
    private transient Map conf;
    private Map<Integer, String> taskToComponentId;

    // componentId -> task set
    private transient Map<String, Set<Integer>> sourceTasks;
    private transient Map<String, Set<Integer>> statefulTasks;
    private transient Map<String, Set<Integer>> nonStatefulTasks;
    private transient Set<Integer> endTasks;

    private BatchStateTracker lastSuccessfulSnapshot;
    private transient RotatingMap<Long, BatchStateTracker> inprogressSnapshots;
    private transient long prevLastSuccessfulBatchId = 0;

    private State state;
    private transient ITopologyStateOperator stateOperator;

    // Map<ComponentId, TaskUserStateInitOperator>
    private transient Map<String, ITaskStateInitOperator> taskStateInitOperators;
    // Map<ComponentId, TaskSysStateInitOperator>
    private transient Map<String, ITaskStateInitOperator> taskSysStateInitOperators;

    public class BatchStateTracker implements Serializable {
        private static final long serialVersionUID = -3503873401193374360L;

        private long batchId;
        private Map<String, Map<Integer, TransactionState>> spouts = new HashMap<String, Map<Integer, TransactionState>>();
        private Map<String, Map<Integer, TransactionState>> statefulBolts = new HashMap<String, Map<Integer, TransactionState>>();
        private transient Map<Integer, Boolean> endBolts = new HashMap<Integer, Boolean>();
        private transient int receivedSpoutCount = 0;
        private transient int receivedStatefulBoltCount = 0;
        private transient int receivedEndBoltCount = 0;
        private transient int expectedSpoutCount;
        private transient int expectedStatefulBoltCount;
        private transient int expectedEndBoltCount;

        public BatchStateTracker(long batchId, Map<String, Set<Integer>> spouts, Map<String, Set<Integer>> statefulBolts, Set<Integer> endBolts) {
            this.batchId = batchId;

            for (Entry<String, Set<Integer>> entry : spouts.entrySet()) {
                Map<Integer, TransactionState> states = new HashMap<Integer, TransactionState>();
                for (Integer taskId : entry.getValue()) {
                    states.put(taskId, null);
                }
                expectedSpoutCount += states.size();
                this.spouts.put(entry.getKey(), states);
            }
            for (Entry<String, Set<Integer>> entry : statefulBolts.entrySet()) {
                Map<Integer, TransactionState> states = new HashMap<Integer, TransactionState>();
                for (Integer taskId : entry.getValue()) {
                    states.put(taskId, null);
                }
                expectedStatefulBoltCount += states.size();
                this.statefulBolts.put(entry.getKey(), states);
            }
            for (Integer taskId : endBolts) {
                this.endBolts.put(taskId, false);
            }
            expectedEndBoltCount += endBolts.size();
        }

        public long getBatchId() {
            return batchId;
        }

        public Map<String, Map<Integer, TransactionState>> getSpouts() {
            return spouts;
        }

        public Map<String, Map<Integer, TransactionState>> getStatefulBolts() {
            return statefulBolts;
        }

        private Map<Integer, TransactionState> flatComponentStates(Map<String, Map<Integer, TransactionState>> componentStats) {
            Map<Integer, TransactionState> ret = new HashMap<>();
            for (Map<Integer, TransactionState> states : componentStats.values()) {
                ret.putAll(states);
            }
            return ret;
        }

        public Map<Integer, TransactionState> getSpoutStates() {
            return flatComponentStates(spouts);
        }

        public Map<Integer, TransactionState> getStatefulBoltStates() {
            return flatComponentStates(statefulBolts);
        }

        public Map<Integer, TransactionState> getComponentStates(String componentId) {
            if (spouts.containsKey(componentId)) {
                return spouts.get(componentId);
            } else if (statefulBolts.containsKey(componentId)) {
                return statefulBolts.get(componentId);
            } else {
                return null;
            }
        }

        public TransactionState getStateByTaskId(int taskId) {
            String componentId = taskToComponentId.get(taskId);
            Map<Integer, TransactionState> componentStates = lastSuccessfulSnapshot.getComponentStates(componentId);
            LOG.debug("taskId={}, componentId={}, states={}, componentStates={}", taskId, componentId, lastSuccessfulSnapshot.statesInfo(), componentStates);
            return  componentStates != null ? componentStates.get(taskId) : null;
        }

        public void updateSpout(int taskId, TransactionState state) {
            Map<Integer, TransactionState> spoutStates = spouts.get(taskToComponentId.get(taskId));

            if (spoutStates.put(taskId, state) != null) {
                LOG.warn("Duplicated state commit for spout-{}, state={}", taskId, state);
            } else {
                receivedSpoutCount++;
            }
        }

        public void updateStatefulBolt(int taskId, TransactionState state) {
            Map<Integer, TransactionState> statefulBoltStates = statefulBolts.get(taskToComponentId.get(taskId));

            if (statefulBoltStates.put(taskId, state) != null) {
                LOG.warn("Duplicated state commit for statefulBolt-{}, state={}", taskId, state);
            } else {
                receivedStatefulBoltCount++;
            }
        }

        public void updateEndBolt(int taskId) {
            if (!endBolts.containsKey(taskId)) {
                LOG.warn("Received unexpected task-{} when updating endBolts", taskId);
                return;
            }

            if (endBolts.put(taskId, true)) {
                LOG.warn("Duplicated ack for endBolt-{}", taskId);
            } else {
                receivedEndBoltCount++;
            }
        }

        public boolean isFinished() {
            if (state.equals(State.ACTIVE) && batchId != TransactionCommon.INIT_BATCH_ID) {
                if (receivedSpoutCount == expectedSpoutCount && receivedStatefulBoltCount == expectedStatefulBoltCount
                        && receivedEndBoltCount == expectedEndBoltCount) {
                    long expectedNextBatchId = getNextExpectedSuccessfulBatch();
                    if (batchId == expectedNextBatchId) {
                        return true;
                    } else {
                        LOG.info("Unexpected forward batch-{} was finished. But the expected is batch-{}", batchId, expectedNextBatchId);
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                // restart is done for rollback or initState
                return receivedEndBoltCount == expectedEndBoltCount;
            }
        }

        @Override
        public String toString() {
            return "batchId=" + batchId + ", receivedSpoutMsgCount=" + receivedSpoutCount + ", receivedStatefulBoltMsgCount=" + receivedStatefulBoltCount
                    + ", receivedEndBoltMsgCount=" + receivedEndBoltCount;
        }

        public String statesInfo() {
            return "[Spout States]: " + spouts + "\n" + "[Bolt States]: " + statefulBolts;
        }

        private Set<Integer> getNotCommittedTasks(Map<String, Map<Integer, TransactionState>> componentToStates) {
            Set<Integer> notCommitTasks = new HashSet<Integer>();
            for (Map<Integer, TransactionState> states : componentToStates.values()) {
                for (Entry<Integer, TransactionState> entry : states.entrySet()) {
                    if (entry.getValue() == null) {
                        notCommitTasks.add(entry.getKey());
                    }
                }
            }
            return notCommitTasks;
        }

        public String hasNotCommittedTasksInfo() {
            Set<Integer> notCommitSpouts = getNotCommittedTasks(spouts);

            Set<Integer> notCommitBolts = getNotCommittedTasks(statefulBolts);

            Set<Integer> notCommitEndBolts = new HashSet<Integer>();
            for (Entry<Integer, Boolean> entry : endBolts.entrySet()) {
                if (!entry.getValue()) {
                    notCommitEndBolts.add(entry.getKey());
                }
            }
            return "Not Committed tasks: Spouts=" + notCommitSpouts + ", Bolts=" + notCommitBolts + ", EndBolts=" + notCommitEndBolts;
        }

        public boolean isAnyCommittedTasks() {
            return receivedSpoutCount != 0 || receivedStatefulBoltCount != 0 || receivedEndBoltCount != 0;
        }
    }

    public SnapshotState() {
        this.state = State.ACTIVE;
    }

    public SnapshotState(TopologyContext context, Map<String, Set<Integer>> spouts, Map<String, Set<Integer>> statefulBolts,
            Map<String, Set<Integer>> nonStatefulBolts, Set<Integer> endBolts, ITopologyStateOperator stateOperator) {
        this.context = context;
        this.conf = context.getStormConf();
        this.taskToComponentId = context.getTaskToComponent();
        this.sourceTasks = spouts;
        this.statefulTasks = statefulBolts;
        this.nonStatefulTasks = nonStatefulBolts;
        this.endTasks = endBolts;

        this.lastSuccessfulSnapshot = new BatchStateTracker(TransactionCommon.INIT_BATCH_ID, spouts, statefulBolts, endBolts);
        this.inprogressSnapshots = new RotatingMap<Long, BatchStateTracker>(3, true);

        this.stateOperator = stateOperator;
        this.taskStateInitOperators = new HashMap<String, ITaskStateInitOperator>();
        LOG.info("");
        Map<String, String> taskStateInitOpRegisterMap = ConfigExtension.getTransactionUserTaskInitRegisterMap(conf);
        if (taskStateInitOpRegisterMap != null) {
            for (Entry<String, String> entry : taskStateInitOpRegisterMap.entrySet()) {
                taskStateInitOperators.put(entry.getKey(), (ITaskStateInitOperator) Utils.newInstance(entry.getValue()));
            }
        }
        this.taskSysStateInitOperators = new HashMap<String, ITaskStateInitOperator>();
        Map<String, String> taskSysStateInitOpRegisterMap = ConfigExtension.getTransactionSysTaskInitRegisterMap(conf);
        if (taskSysStateInitOpRegisterMap != null) {
            for (Entry<String, String> entry : taskSysStateInitOpRegisterMap.entrySet()) {
                taskSysStateInitOperators.put(entry.getKey(), (ITaskStateInitOperator) Utils.newInstance(entry.getValue()));
            }
        }

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
     * @param batchId batch id
     * @param taskId task id
     * @param state state
     * @return true if current batch is done
     */
    public boolean commit(long batchId, int taskId, TransactionState state) {
        // Ingore any batches' ack when current state is NOT active
        if (batchId != TransactionCommon.INIT_BATCH_ID && !isActive())
            return false;

        String componentId = taskToComponentId.get(taskId);
        if (sourceTasks.containsKey(componentId)) {
            return commitSource(batchId, taskId, state);
        } else if (statefulTasks.containsKey(componentId)) {
            return commitStatefulBolt(batchId, taskId, state);
        } else {
            return false;
        }
    }

    /**
     * @param batchId batch id
     * @param taskId task id
     * @param state state
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
     * @param batchId batch id
     * @param taskId task id
     * @param state state
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
     * @param batchId batch id
     * @param taskId task id
     * @return true if current batch is done
     */
    public boolean ackEndBolt(long batchId, int taskId) {
        // Ingore any batches' ack when current state is NOT active
        if (batchId != TransactionCommon.INIT_BATCH_ID && !isActive())
            return false;

        BatchStateTracker stateTracker = getStateTracker(batchId);
        if (stateTracker != null) {
            stateTracker.updateEndBolt(taskId);
            return stateTracker.isFinished();
        } else {
            return false;
        }
    }

    /**
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
            for (Set<Integer> spoutTasks : sourceTasks.values()) {
                for (Integer taskId : spoutTasks) {
                    snapshotState.put(taskId, null);
                }
            }
            for (Set<Integer> boltTasks : statefulTasks.values()) {
                for (Integer taskId : boltTasks) {
                    snapshotState.put(taskId, null);
                }
            }
        }
        return snapshotState;
    }

    private TransactionState getRebuiltState(int taskId, BatchStateTracker lastSuccessfulSnapshot) {
        Object systemState = null;
        Object userState = null;

        long batchId = lastSuccessfulSnapshot.getBatchId();
        String componentId = taskToComponentId.get(taskId);
        Map<Integer, TransactionState> componentStates = lastSuccessfulSnapshot.getComponentStates(componentId);
        Set<Integer> currTasks = new HashSet<Integer>(context.getComponentTasks(componentId));
        TransactionState state = componentStates != null ? componentStates.get(taskId) : null;
        LOG.debug("States of component={}: {}", componentId, componentStates);
        LOG.debug("taskStateInitOperators: {}", taskStateInitOperators);
        LOG.debug("Prev state of task-{}: {}", taskId, state);
        LOG.debug("currTasks: {}", currTasks);

        // Get task's system state
        ITaskStateInitOperator taskSysInitOperator = taskSysStateInitOperators.get(componentId);
        if (taskSysInitOperator != null) {
            taskSysInitOperator.getTaskInitState(conf, taskId, currTasks, componentStates);
        } else {
            if (stateOperator instanceof ITopologyStateInitOperator) {
                ITopologyStateInitOperator richStateOperator = (ITopologyStateInitOperator) stateOperator;
                if (sourceTasks.containsKey(componentId)) {
                    systemState = richStateOperator.getInitSpoutSysState(taskId, currTasks, componentStates);
                } else if (statefulTasks.containsKey(componentId)) {
                    systemState = richStateOperator.getInitBoltSysState(taskId, currTasks, componentStates);
                }
            } else {
                systemState = state != null ? state.systemCheckpoint : null;
            }
        }

        // get task's user state
        ITaskStateInitOperator taskInitOperator = taskStateInitOperators.get(componentId);
        if (taskInitOperator != null) {
            userState = taskInitOperator.getTaskInitState(conf, taskId, currTasks, componentStates);
        } else {
            if (stateOperator instanceof ITopologyStateInitOperator) {
                ITopologyStateInitOperator richStateOperator = (ITopologyStateInitOperator) stateOperator;
                if (sourceTasks.containsKey(componentId)) {
                    userState = richStateOperator.getInitSpoutUserState(taskId, sourceTasks.get(componentId), componentStates);
                } else if (statefulTasks.containsKey(componentId)) {
                    userState = richStateOperator.getInitBoltUserState(taskId, statefulTasks.get(componentId), componentStates);
                }
            } else {
                userState = state != null ? state.userCheckpoint : null;
            }
        }

        TransactionState ret = new TransactionState(batchId, systemState, userState);
        LOG.debug("Initial state={}", ret);
        return ret;
    }

    private void rebuildComponentStates(Map<Integer, TransactionState> componentStates, BatchStateTracker lastSuccessfulSnapshot) {
        for (Entry<Integer, TransactionState> entry : componentStates.entrySet()) {
            Integer taskId = entry.getKey();
            entry.setValue(getRebuiltState(taskId, lastSuccessfulSnapshot));
        }
    }

    public TransactionState getInitState(int taskId) {
        // If the task does not belong to this snapshot state, just return null;
        if (!containsTask(taskId)) {
            LOG.warn("Received unexpected init state request from task-{}", taskId);
            return null;
        }

        /*TransactionState ret = null;
        if (inprogressSnapshots.size() > 0)
            ret = getLastestCommittedState(taskId);
        if (ret == null)
            ret = getLastSuccessState(taskId);
        return ret;*/
        return getLastSuccessState(taskId);
    }

    private TransactionState getLastestCommittedState(int taskId) {
        TransactionState ret = null;
        String componentId = context.getComponentId(taskId);
        TreeSet<Long> batchIds = new TreeSet<Long>(inprogressSnapshots.keySet());
        Long batchId = null;
        while ((batchId = batchIds.pollLast()) != null) {
            BatchStateTracker tracker = inprogressSnapshots.get(batchId);
            Map<Integer, TransactionState> states = tracker.getComponentStates(componentId);
            if (states != null && (ret = states.get(taskId)) != null)
                break;
        }
        return ret;
    }

    private TransactionState getLastSuccessState(int taskId) {
        TransactionState state = lastSuccessfulSnapshot.getStateByTaskId(taskId);
        return  state != null ? state : new TransactionState(lastSuccessfulSnapshot.batchId);
    }

    public void successBatch(long batchId) {
        long nextBatchId = 1;
        if (batchId == TransactionCommon.INIT_BATCH_ID) {
            inprogressSnapshots.remove(batchId);
            if (lastSuccessfulSnapshot != null)
                nextBatchId =  lastSuccessfulSnapshot.batchId + 1;
            
        } else {
            long id = lastSuccessfulSnapshot.getBatchId() + 1;
            for (; id < batchId; id++) {
                inprogressSnapshots.remove(id);
            }
            lastSuccessfulSnapshot = (BatchStateTracker) inprogressSnapshots.remove(batchId);

            nextBatchId = batchId + 1;
        }

        // Prepare to track next batch
        if (!inprogressSnapshots.containsKey(nextBatchId)) {
            getStateTracker(nextBatchId);
        }
    }

    private long getNextExpectedSuccessfulBatch() {
        return lastSuccessfulSnapshot.getBatchId() + 1;
    }

    /**
     * @return -1: none pending successful batch >1: batch id of next pending successful batch
     */
    public long getPendingSuccessBatch() {
        long nextBatchId = getNextExpectedSuccessfulBatch();
        BatchStateTracker nextBatch = inprogressSnapshots.get(nextBatchId);
        if (nextBatch == null) {
            return -1;
        } else {
            if (nextBatch.isFinished()) {
                return nextBatch.batchId;
            } else {
                return -1;
            }
        }
    }

    private Set<Integer> flatComponentsToTasks(Map<String, Set<Integer>> componentToTasks) {
        Set<Integer> ret = new HashSet<Integer>();
        for (Set<Integer> tasks : componentToTasks.values()) {
            ret.addAll(tasks);
        }
        return ret;
    }

    private boolean containsTask(int taskId) {
        String componentId = taskToComponentId.get(taskId);
        if (sourceTasks.containsKey(componentId)) {
            return true;
        } else if (statefulTasks.containsKey(componentId)) {
            return true;
        } else if (nonStatefulTasks.containsKey(componentId)) {
            return true;
        } else {
            return false;
        }
    }

    public Set<Integer> getSpoutTasks() {
        return flatComponentsToTasks(sourceTasks);
    }

    public Set<Integer> getStatefulTasks() {
        return flatComponentsToTasks(statefulTasks);
    }

    public Set<Integer> getNonStatefulTasks() {
        return flatComponentsToTasks(nonStatefulTasks);
    }

    /**
     * @return Map[taskId, LastSuccessfulTransactionState]
     */
    public Map<Integer, TransactionState> expiredCheck() {
        Map<Integer, TransactionState> rollbackSnapshots = new HashMap<Integer, TransactionState>();
        Map<Long, BatchStateTracker> expiredSnapshots = inprogressSnapshots.rotate();
        if (expiredSnapshots.size() > 0) {
            LOG.info("Found expired batch!");
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

    public boolean isRollback() {
        return state.equals(State.ROLLBACK);
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

    public void initState(SnapshotState state) {
        long nextBatchId = 1;
        if (state != null) {
            /**
             * rebuild last successful state tracker in case of any scaling-out/in
             */
            BatchStateTracker oldLastSuccessStateTracker = state.getLastSuccessfulBatch();
            BatchStateTracker newLastSuccessStateTracker = new BatchStateTracker(oldLastSuccessStateTracker.batchId, sourceTasks, statefulTasks, endTasks);
            // rebuild spout states by old last successful states
            Map<String, Map<Integer, TransactionState>> spouts = newLastSuccessStateTracker.getSpouts();
            for (Map<Integer, TransactionState> componentStates : spouts.values()) {
                rebuildComponentStates(componentStates, oldLastSuccessStateTracker);
            }
            // rebuild stateful bolt states by old last successful states
            Map<String, Map<Integer, TransactionState>> statefulBolts = newLastSuccessStateTracker.getStatefulBolts();
            for (Map<Integer, TransactionState> componentStates : statefulBolts.values()) {
                rebuildComponentStates(componentStates, oldLastSuccessStateTracker);
            }

            setLastSuccessfulBatch(newLastSuccessStateTracker);
            LOG.info("Old statesInfo: {}", oldLastSuccessStateTracker.statesInfo());
            LOG.info("New statesInfo: {}", newLastSuccessStateTracker.statesInfo());

            setState(state.getState());
            nextBatchId = state.getLastSuccessfulBatch().batchId + 1;
        }
        // Move to next batch
        getStateTracker(nextBatchId);
    }

    public BatchStateTracker getLastSuccessfulBatch() {
        return lastSuccessfulSnapshot;
    }

    public void setLastSuccessfulBatch(BatchStateTracker tracker) {
        this.lastSuccessfulSnapshot = tracker;
        this.prevLastSuccessfulBatchId = tracker.getBatchId();
    }

    public long getLastSuccessfulBatchId() {
        return lastSuccessfulSnapshot != null ? lastSuccessfulSnapshot.getBatchId() : 0;
    }

    public void setStateOperator(ITopologyStateOperator op) {
        this.stateOperator = op;
    }

    public boolean isRunning() {
        return prevLastSuccessfulBatchId != getLastSuccessfulBatchId();
    }

    @Override
    public String toString() {
        return "state=" + state.toString() + ", sourceTasks=" + sourceTasks + ", statefulTasks=" + statefulTasks + ", nonStatefulTasks" + nonStatefulTasks
                + ", endTasks=" + endTasks + ", inprogressSnapshots" + inprogressSnapshots + ", lastSuccessfulBatchId=" + lastSuccessfulSnapshot.batchId;
    }
}