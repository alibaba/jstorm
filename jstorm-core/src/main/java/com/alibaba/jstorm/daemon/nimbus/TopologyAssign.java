/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.metric.JStormMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.TaskStartEvent;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.AssignmentBak;
import com.alibaba.jstorm.schedule.IToplogyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.DefaultTopologyScheduler;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.WorkerSlot;

public class TopologyAssign implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(TopologyAssign.class);

    /**
     * private constructor function to ensure singleton
     */
    private TopologyAssign() {
    }

    private static TopologyAssign instance = null;

    public static TopologyAssign getInstance() {
        synchronized (TopologyAssign.class) {
            if (instance == null) {
                instance = new TopologyAssign();
            }
            return instance;
        }
    }

    protected NimbusData nimbusData;
    protected Map<String, IToplogyScheduler> schedulers;
    private Thread thread;
    public static final String DEFAULT_SCHEDULER_NAME = "default";

    public void init(NimbusData nimbusData) {
        this.nimbusData = nimbusData;
        this.schedulers = new HashMap<>();

        IToplogyScheduler defaultScheduler = new DefaultTopologyScheduler();
        defaultScheduler.prepare(nimbusData.getConf());

        schedulers.put(DEFAULT_SCHEDULER_NAME, defaultScheduler);

        thread = new Thread(this);
        thread.setName("TopologyAssign");
        thread.setDaemon(true);
        thread.start();
    }

    public void cleanup() {
        runFlag = false;
        thread.interrupt();
    }

    protected static LinkedBlockingQueue<TopologyAssignEvent> queue = new LinkedBlockingQueue<TopologyAssignEvent>();

    public static void push(TopologyAssignEvent event) {
        queue.offer(event);
    }

    volatile boolean runFlag = false;

    public void run() {
        LOG.info("TopologyAssign thread has been started");
        runFlag = true;

        while (runFlag) {
            TopologyAssignEvent event;
            try {
                event = queue.take();
            } catch (InterruptedException e1) {
                continue;
            }
            if (event == null) {
                continue;
            }

            boolean isSuccess = doTopologyAssignment(event);
            if (isSuccess) {
                try {
                    cleanupDisappearedTopology();
                } catch (Exception e) {
                    LOG.error("Failed to do cleanup disappeared topology ", e);
                }
            }
        }

    }

    protected void pushTaskStartEvent(Assignment oldAssignment, Assignment newAssignment, TopologyAssignEvent event)
            throws Exception {
        // notify jstorm monitor on task assign/reassign/rebalance
        TaskStartEvent taskEvent = new TaskStartEvent();
        taskEvent.setOldAssignment(oldAssignment);
        taskEvent.setNewAssignment(newAssignment);
        taskEvent.setTopologyId(event.getTopologyId());

        Map<Integer, String> task2Component;
        // get from nimbus cache first
        Map<Integer, TaskInfo> taskInfoMap = Cluster.get_all_taskInfo(nimbusData.getStormClusterState(),
                event.getTopologyId());
        if (taskInfoMap != null) {
            task2Component = Common.getTaskToComponent(taskInfoMap);
        } else {
            task2Component = Common.getTaskToComponent(
                    Cluster.get_all_taskInfo(nimbusData.getStormClusterState(), event.getTopologyId()));
        }
        taskEvent.setTask2Component(task2Component);
        ClusterMetricsRunnable.pushEvent(taskEvent);
    }

    /**
     * Create/Update topology assignment set topology status
     */
    protected boolean doTopologyAssignment(TopologyAssignEvent event) {
        Assignment assignment;
        try {
            Assignment oldAssignment = null;
            boolean isReassign = event.isScratch();
            if (isReassign) {
                oldAssignment = nimbusData.getStormClusterState().assignment_info(event.getTopologyId(), null);
            }
            assignment = mkAssignment(event);

            pushTaskStartEvent(oldAssignment, assignment, event);

            if (!isReassign) {
                setTopologyStatus(event);
            }
        } catch (Throwable e) {
            LOG.error("Failed to assign topology " + event.getTopologyId(), e);
            event.fail(e.getMessage());
            return false;
        }

        if (assignment != null)
            backupAssignment(assignment, event);
        event.done();
        return true;
    }

    /**
     * cleanup the topologies which are not in ZK /topology, but in other place
     */
    public void cleanupDisappearedTopology() throws Exception {
        StormClusterState clusterState = nimbusData.getStormClusterState();

        List<String> activeTopologies = clusterState.active_storms();
        if (activeTopologies == null) {
            return;
        }

        Set<String> cleanupIds = get_cleanup_ids(clusterState, activeTopologies);
        for (String sysTopology : JStormMetrics.SYS_TOPOLOGIES) {
            cleanupIds.remove(sysTopology);
        }
        for (String topologyId : cleanupIds) {
            LOG.info("Cleaning up " + topologyId);
            clusterState.try_remove_storm(topologyId);
            nimbusData.getTaskHeartbeatsCache().remove(topologyId);
            nimbusData.getTasksHeartbeat().remove(topologyId);

            NimbusUtils.removeTopologyTaskTimeout(nimbusData, topologyId);

            // delete topology files in blobstore
            List<String> deleteKeys = BlobStoreUtils.getKeyListFromId(nimbusData, topologyId);
            BlobStoreUtils.cleanup_keys(deleteKeys, nimbusData.getBlobStore(), nimbusData.getStormClusterState());

            // don't need to delete local dir
        }
    }

    /**
     * get topology ids that need to be cleaned up
     */
    private Set<String> get_cleanup_ids(StormClusterState clusterState, List<String> activeTopologies) throws Exception {
        List<String> task_ids = clusterState.task_storms();
        List<String> heartbeat_ids = clusterState.heartbeat_storms();
        List<String> error_ids = clusterState.task_error_storms();
        List<String> assignment_ids = clusterState.assignments(null);
        List<String> metric_ids = clusterState.get_metrics();

        HashSet<String> latest_code_ids = new HashSet<>();

        Set<String> code_ids = BlobStoreUtils.code_ids(nimbusData.getBlobStore());
        Set<String> to_cleanup_ids = new HashSet<>();
        Set<String> pendingTopologies = nimbusData.getPendingSubmitTopologies().buildMap().keySet();

        if (task_ids != null) {
            to_cleanup_ids.addAll(task_ids);
        }

        if (heartbeat_ids != null) {
            to_cleanup_ids.addAll(heartbeat_ids);
        }

        if (error_ids != null) {
            to_cleanup_ids.addAll(error_ids);
        }

        if (assignment_ids != null) {
            to_cleanup_ids.addAll(assignment_ids);
        }

        if (code_ids != null) {
            to_cleanup_ids.addAll(code_ids);
        }

        if (metric_ids != null) {
            to_cleanup_ids.addAll(metric_ids);
        }

        if (activeTopologies != null) {
            to_cleanup_ids.removeAll(activeTopologies);
            latest_code_ids.removeAll(activeTopologies);
        }

        to_cleanup_ids.removeAll(pendingTopologies);

        /**
         * Why need to remove latest code. Due to competition between Thrift.threads and TopologyAssign thread
         */
        to_cleanup_ids.removeAll(latest_code_ids);
        LOG.info("Skip removing topology of " + latest_code_ids);

        return to_cleanup_ids;
    }

    /**
     * start a topology: set active status of the topology
     */
    public void setTopologyStatus(TopologyAssignEvent event) throws Exception {
        StormClusterState stormClusterState = nimbusData.getStormClusterState();

        String topologyId = event.getTopologyId();
        String topologyName = event.getTopologyName();
        String group = event.getGroup();

        StormStatus status = new StormStatus(StatusType.active);
        if (event.getOldStatus() != null) {
            status = event.getOldStatus();
        }

        boolean isEnable = ConfigExtension.isEnablePerformanceMetrics(nimbusData.getConf());
        StormBase stormBase = stormClusterState.storm_base(topologyId, null);
        if (stormBase == null) {
            stormBase = new StormBase(topologyName, TimeUtils.current_time_secs(), status, group);
            stormBase.setEnableMonitor(isEnable);
            stormClusterState.activate_storm(topologyId, stormBase);
        } else {
            stormClusterState.update_storm(topologyId, status);
            stormClusterState.set_storm_monitor(topologyId, isEnable);

            // here exist one hack operation
            // when monitor/rebalance/startup topologyName is null
            if (topologyName == null) {
                event.setTopologyName(stormBase.getStormName());
            }
        }

        LOG.info("Update " + topologyId + " " + status);
    }

    protected TopologyAssignContext prepareTopologyAssign(TopologyAssignEvent event) throws Exception {
        TopologyAssignContext ret = new TopologyAssignContext();

        String topologyId = event.getTopologyId();
        ret.setTopologyId(topologyId);

        int topoMasterId = nimbusData.getTasksHeartbeat().get(topologyId).get_topologyMasterId();
        ret.setTopologyMasterTaskId(topoMasterId);
        LOG.info("prepareTopologyAssign, topoMasterId={}", topoMasterId);

        Map<Object, Object> nimbusConf = nimbusData.getConf();
        Map<Object, Object> topologyConf = StormConfig.read_nimbus_topology_conf(topologyId, nimbusData.getBlobStore());

        StormTopology rawTopology = StormConfig.read_nimbus_topology_code(topologyId, nimbusData.getBlobStore());
        ret.setRawTopology(rawTopology);

        Map stormConf = new HashMap();
        LOG.info("GET RESERVE_WORKERS from ={}", Utils.readDefaultConfig());
        LOG.info("RESERVE_WORKERS ={}", Utils.readDefaultConfig().get(Config.RESERVE_WORKERS));
        stormConf.put(Config.RESERVE_WORKERS, Utils.readDefaultConfig().get(Config.RESERVE_WORKERS));
        stormConf.putAll(nimbusConf);
        stormConf.putAll(topologyConf);
        ret.setStormConf(stormConf);

        StormClusterState stormClusterState = nimbusData.getStormClusterState();

        // get all running supervisor, don't need callback to watch supervisor
        Map<String, SupervisorInfo> supInfos = Cluster.get_all_SupervisorInfo(stormClusterState, null);
        // init all AvailableWorkerPorts
        for (Entry<String, SupervisorInfo> supInfo : supInfos.entrySet()) {
            SupervisorInfo supervisor = supInfo.getValue();
            if (supervisor != null)
                supervisor.setAvailableWorkerPorts(supervisor.getWorkerPorts());
        }

        getAliveSupervsByHb(supInfos, nimbusConf);
        if (supInfos.size() == 0) {
            throw new FailedAssignTopologyException("Failed to make assignment " + topologyId + ", due to no alive supervisor");
        }

        Map<Integer, String> taskToComponent = Cluster.get_all_task_component(stormClusterState, topologyId, null);
        ret.setTaskToComponent(taskToComponent);

        // get taskids /ZK/tasks/topologyId
        Set<Integer> allTaskIds = taskToComponent.keySet();
        if (allTaskIds.size() == 0) {
            String errMsg = "Failed to get all task ID list from /ZK-dir/tasks/" + topologyId;
            LOG.warn(errMsg);
            throw new IOException(errMsg);
        }
        ret.setAllTaskIds(allTaskIds);

        Set<Integer> aliveTasks = new HashSet<>();
        // unstoppedTasks are tasks which are alive on no supervisor's(dead)
        // machine
        Set<Integer> unstoppedTasks = new HashSet<>();
        Set<Integer> deadTasks = new HashSet<>();
        Set<ResourceWorkerSlot> unstoppedWorkers;

        Assignment existingAssignment = stormClusterState.assignment_info(topologyId, null);
        if (existingAssignment != null) {
            /*
             * Check if the topology master task is alive first since all task 
             * heartbeat info is reported by topology master. 
             * If master is dead, do reassignment for topology master first.
             */
            if (NimbusUtils.isTaskDead(nimbusData, topologyId, topoMasterId)) {
                ResourceWorkerSlot tmWorker = existingAssignment.getWorkerByTaskId(topoMasterId);
                deadTasks.addAll(tmWorker.getTasks());
            } else {
                deadTasks = getDeadTasks(topologyId, allTaskIds, existingAssignment.getWorkers());
            }
            aliveTasks.addAll(allTaskIds);
            aliveTasks.removeAll(deadTasks);

            unstoppedTasks = getUnstoppedSlots(aliveTasks, supInfos, existingAssignment);
        }

        ret.setDeadTaskIds(deadTasks);
        ret.setUnstoppedTaskIds(unstoppedTasks);

        // Step 2: get all slots resource, free slots/ alive slots/ unstopped
        // slots
        getFreeSlots(supInfos, stormClusterState);
        ret.setCluster(supInfos);

        if (existingAssignment == null) {
            ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_NEW);

            try {
                AssignmentBak lastAssignment = stormClusterState.assignment_bak(event.getTopologyName());
                if (lastAssignment != null) {
                    ret.setOldAssignment(lastAssignment.getAssignment());
                }
            } catch (Exception e) {
                LOG.warn("Fail to get old assignment", e);
            }
        } else {
            ret.setOldAssignment(existingAssignment);
            if (event.isScratch()) {
                ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_REBALANCE);
                ret.setIsReassign(event.isReassign());
                unstoppedWorkers = getUnstoppedWorkers(unstoppedTasks, existingAssignment);
                ret.setUnstoppedWorkers(unstoppedWorkers);
            } else {
                ret.setAssignType(TopologyAssignContext.ASSIGN_TYPE_MONITOR);
                unstoppedWorkers = getUnstoppedWorkers(aliveTasks, existingAssignment);
                ret.setUnstoppedWorkers(unstoppedWorkers);
            }
        }

        return ret;
    }

    /**
     * make assignments for a topology The nimbus core function, this function has been totally rewrite
     *
     * @throws Exception
     */
    public Assignment mkAssignment(TopologyAssignEvent event) throws Exception {
        String topologyId = event.getTopologyId();
        LOG.info("Determining assignment for " + topologyId);
        TopologyAssignContext context = prepareTopologyAssign(event);
        Set<ResourceWorkerSlot> assignments;
        if (!StormConfig.local_mode(nimbusData.getConf())) {
            IToplogyScheduler scheduler = schedulers.get(DEFAULT_SCHEDULER_NAME);
            assignments = scheduler.assignTasks(context);
        } else {
            assignments = mkLocalAssignment(context);
        }

        Assignment assignment = null;
        if (assignments != null && assignments.size() > 0) {
            Map<String, String> nodeHost = getTopologyNodeHost(
                    context.getCluster(), context.getOldAssignment(), assignments);
            Map<Integer, Integer> startTimes = getTaskStartTimes(
                    context, nimbusData, topologyId, context.getOldAssignment(), assignments);

            String codeDir = (String) nimbusData.getConf().get(Config.STORM_LOCAL_DIR);

            assignment = new Assignment(codeDir, assignments, nodeHost, startTimes);

            //  the topology binary changed.
            if (event.isScaleTopology()) {
                assignment.setAssignmentType(Assignment.AssignmentType.ScaleTopology);
            }
            StormClusterState stormClusterState = nimbusData.getStormClusterState();

            stormClusterState.set_assignment(topologyId, assignment);

            // update task heartbeat's start time
            NimbusUtils.updateTaskHbStartTime(nimbusData, assignment, topologyId);

            // Update metrics information in ZK when rebalance or reassignment
            // Only update metrics monitor status when creating topology
            // if (context.getAssignType() ==
            // TopologyAssignContext.ASSIGN_TYPE_REBALANCE
            // || context.getAssignType() ==
            // TopologyAssignContext.ASSIGN_TYPE_MONITOR)
            // NimbusUtils.updateMetricsInfo(nimbusData, topologyId, assignment);

            NimbusUtils.updateTopologyTaskTimeout(nimbusData, topologyId);

            LOG.info("Successfully make assignment for topology id " + topologyId + ": " + assignment);
        }
        return assignment;
    }

    private static Set<ResourceWorkerSlot> mkLocalAssignment(TopologyAssignContext context) throws Exception {
        Set<ResourceWorkerSlot> result = new HashSet<>();
        Map<String, SupervisorInfo> cluster = context.getCluster();
        if (cluster.size() != 1)
            throw new RuntimeException();
        SupervisorInfo localSupervisor = null;
        String supervisorId = null;
        for (Entry<String, SupervisorInfo> entry : cluster.entrySet()) {
            supervisorId = entry.getKey();
            localSupervisor = entry.getValue();
        }
        int port;
        if (localSupervisor.getAvailableWorkerPorts().iterator().hasNext()) {
            port = localSupervisor.getAvailableWorkerPorts().iterator().next();
        } else {
            LOG.info("The amount of worker ports is not enough");
            throw new FailedAssignTopologyException(
                    "Failed to make assignment " + ", due to no enough ports");
        }

        ResourceWorkerSlot worker = new ResourceWorkerSlot(supervisorId, port);
        worker.setTasks(new HashSet<>(context.getAllTaskIds()));
        worker.setHostname(localSupervisor.getHostName());
        result.add(worker);
        return result;
    }

    public static Map<Integer, Integer> getTaskStartTimes(TopologyAssignContext context, NimbusData nimbusData,
                                                          String topologyId, Assignment existingAssignment,
                                                          Set<ResourceWorkerSlot> workers) throws Exception {
        Map<Integer, Integer> startTimes = new TreeMap<>();

        if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_NEW) {
            int nowSecs = TimeUtils.current_time_secs();
            for (ResourceWorkerSlot worker : workers) {
                for (Integer changedTaskId : worker.getTasks()) {
                    startTimes.put(changedTaskId, nowSecs);
                }
            }

            return startTimes;
        }

        Set<ResourceWorkerSlot> oldWorkers = new HashSet<>();
        if (existingAssignment != null) {
            Map<Integer, Integer> taskStartTimeSecs = existingAssignment.getTaskStartTimeSecs();
            if (taskStartTimeSecs != null) {
                startTimes.putAll(taskStartTimeSecs);
            }

            if (existingAssignment.getWorkers() != null) {
                oldWorkers = existingAssignment.getWorkers();
            }
        }

        Set<Integer> changedTaskIds = getNewOrChangedTaskIds(oldWorkers, workers);
        int nowSecs = TimeUtils.current_time_secs();
        for (Integer changedTaskId : changedTaskIds) {
            startTimes.put(changedTaskId, nowSecs);
            NimbusUtils.removeTopologyTaskHb(nimbusData, topologyId, changedTaskId);
        }

        Set<Integer> removedTaskIds = getRemovedTaskIds(oldWorkers, workers);
        for (Integer removedTaskId : removedTaskIds) {
            startTimes.remove(removedTaskId);
            NimbusUtils.removeTopologyTaskHb(nimbusData, topologyId, removedTaskId);
        }

        LOG.info("Task assignment has been changed: " + changedTaskIds + ", removed tasks " + removedTaskIds);
        return startTimes;
    }

    public static Map<String, String> getTopologyNodeHost(
            Map<String, SupervisorInfo> supervisorMap, Assignment existingAssignment, Set<ResourceWorkerSlot> workers) {
        // the following is that remove unused node from allNodeHost
        Set<String> usedNodes = new HashSet<>();
        for (ResourceWorkerSlot worker : workers) {
            usedNodes.add(worker.getNodeId());
        }

        // map<supervisorId, hostname>
        Map<String, String> allNodeHost = new HashMap<>();
        if (existingAssignment != null) {
            allNodeHost.putAll(existingAssignment.getNodeHost());
        }

        // get alive supervisorMap Map<supervisorId, hostname>
        Map<String, String> nodeHost = SupervisorInfo.getNodeHost(supervisorMap);
        if (nodeHost != null) {
            allNodeHost.putAll(nodeHost);
        }

        Map<String, String> ret = new HashMap<>();
        for (String supervisorId : usedNodes) {
            if (allNodeHost.containsKey(supervisorId)) {
                ret.put(supervisorId, allNodeHost.get(supervisorId));
            } else {
                LOG.warn("Node " + supervisorId + " doesn't in the supervisor list");
            }
        }

        return ret;
    }

    /**
     * get all task ids which are newly assigned or reassigned
     */
    public static Set<Integer> getNewOrChangedTaskIds(Set<ResourceWorkerSlot> oldWorkers, Set<ResourceWorkerSlot> workers) {
        Set<Integer> rtn = new HashSet<>();
        HashMap<String, ResourceWorkerSlot> workerPortMap = HostPortToWorkerMap(oldWorkers);
        for (ResourceWorkerSlot worker : workers) {
            ResourceWorkerSlot oldWorker = workerPortMap.get(worker.getHostPort());
            if (oldWorker != null) {
                Set<Integer> oldTasks = oldWorker.getTasks();
                for (Integer task : worker.getTasks()) {
                    if (!(oldTasks.contains(task)))
                        rtn.add(task);
                }
            } else {
                if (worker.getTasks() != null) {
                    rtn.addAll(worker.getTasks());
                }
            }
        }
        return rtn;
    }

    public static Set<Integer> getRemovedTaskIds(Set<ResourceWorkerSlot> oldWorkers, Set<ResourceWorkerSlot> workers) {
        Set<Integer> rtn = new HashSet<>();
        Set<Integer> oldTasks = getTaskSetFromWorkerSet(oldWorkers);
        Set<Integer> newTasks = getTaskSetFromWorkerSet(workers);
        for (Integer taskId : oldTasks) {
            if (!(newTasks.contains(taskId))) {
                rtn.add(taskId);
            }
        }
        return rtn;
    }

    private static Set<Integer> getTaskSetFromWorkerSet(Set<ResourceWorkerSlot> workers) {
        Set<Integer> rtn = new HashSet<>();
        for (ResourceWorkerSlot worker : workers) {
            rtn.addAll(worker.getTasks());
        }
        return rtn;
    }

    private static HashMap<String, ResourceWorkerSlot> HostPortToWorkerMap(Set<ResourceWorkerSlot> workers) {
        HashMap<String, ResourceWorkerSlot> rtn = new HashMap<>();
        for (ResourceWorkerSlot worker : workers) {
            rtn.put(worker.getHostPort(), worker);
        }
        return rtn;
    }

    /**
     * sort slots, the purpose is to ensure that the tasks are assigned in balancing
     *
     * @return List<WorkerSlot>
     */
    public static List<WorkerSlot> sortSlots(Set<WorkerSlot> allSlots, int needSlotNum) {
        Map<String, List<WorkerSlot>> nodeMap = new HashMap<>();

        // group by first
        for (WorkerSlot np : allSlots) {
            String node = np.getNodeId();
            List<WorkerSlot> list = nodeMap.get(node);
            if (list == null) {
                list = new ArrayList<>();
                nodeMap.put(node, list);
            }

            list.add(np);
        }

        for (Entry<String, List<WorkerSlot>> entry : nodeMap.entrySet()) {
            List<WorkerSlot> ports = entry.getValue();
            Collections.sort(ports, new Comparator<WorkerSlot>() {

                @Override
                public int compare(WorkerSlot first, WorkerSlot second) {
                    String firstNode = first.getNodeId();
                    String secondNode = second.getNodeId();
                    if (!firstNode.equals(secondNode)) {
                        return firstNode.compareTo(secondNode);
                    } else {
                        return first.getPort() - second.getPort();
                    }
                }

            });
        }

        // interleave
        List<List<WorkerSlot>> splitup = new ArrayList<>(nodeMap.values());
        Collections.sort(splitup, new Comparator<List<WorkerSlot>>() {
            public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                return o2.size() - o1.size();
            }
        });

        List<WorkerSlot> sortedFreeSlots = JStormUtils.interleave_all(splitup);

        if (sortedFreeSlots.size() <= needSlotNum) {
            return sortedFreeSlots;
        }

        // sortedFreeSlots > needSlotNum
        return sortedFreeSlots.subList(0, needSlotNum);
    }

    /**
     * Get unstopped slots from alive task list
     */
    public Set<Integer> getUnstoppedSlots(Set<Integer> aliveTasks, Map<String, SupervisorInfo> supInfos,
                                          Assignment existAssignment) {
        Set<Integer> ret = new HashSet<>();

        Set<ResourceWorkerSlot> oldWorkers = existAssignment.getWorkers();
        Set<String> aliveSupervisors = supInfos.keySet();
        for (ResourceWorkerSlot worker : oldWorkers) {
            for (Integer taskId : worker.getTasks()) {
                if (!aliveTasks.contains(taskId)) {
                    // task is dead
                    continue;
                }

                String oldTaskSupervisorId = worker.getNodeId();
                if (!aliveSupervisors.contains(oldTaskSupervisorId)) {
                    // supervisor is dead
                    ret.add(taskId);
                }
            }
        }

        return ret;
    }

    private Set<ResourceWorkerSlot> getUnstoppedWorkers(Set<Integer> aliveTasks, Assignment existAssignment) {
        Set<ResourceWorkerSlot> ret = new HashSet<>();
        for (ResourceWorkerSlot worker : existAssignment.getWorkers()) {
            boolean alive = true;
            for (Integer task : worker.getTasks()) {
                if (!aliveTasks.contains(task)) {
                    alive = false;
                    break;
                }
            }
            if (alive) {
                ret.add(worker);
            }
        }
        return ret;
    }

    /**
     * Get free resources
     */
    public static void getFreeSlots(Map<String, SupervisorInfo> supervisorInfos,
                                    StormClusterState stormClusterState) throws Exception {
        Map<String, Assignment> assignments = Cluster.get_all_assignment(stormClusterState, null);
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            Assignment assignment = entry.getValue();
            Set<ResourceWorkerSlot> workers = assignment.getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                SupervisorInfo supervisorInfo = supervisorInfos.get(worker.getNodeId());
                if (supervisorInfo == null) {
                    // the supervisor is dead
                    continue;
                }
                supervisorInfo.getAvailableWorkerPorts().remove(worker.getPort());
            }
        }
    }

    /**
     * find all alive task ids. Do not assume that clocks are synchronized.
     * Task heartbeat is only used so that nimbus knows when it's received a new heartbeat.
     * All timing is done by nimbus and tracked through task-heartbeat-cache
     *
     * @return Set<Integer> : task id set
     */
    public Set<Integer> getAliveTasks(String topologyId, Set<Integer> taskIds) throws Exception {
        Set<Integer> aliveTasks = new HashSet<>();

        // taskIds is the list from ZK /ZK-DIR/tasks/topologyId
        for (int taskId : taskIds) {
            boolean isDead = NimbusUtils.isTaskDead(nimbusData, topologyId, taskId);
            if (!isDead) {
                aliveTasks.add(taskId);
            }
        }
        return aliveTasks;
    }

    public Set<Integer> getDeadTasks(String topologyId, Set<Integer> allTaskIds, Set<ResourceWorkerSlot> allWorkers) throws Exception {
        Set<Integer> deadTasks = new HashSet<>();
        // Get all tasks whose heartbeat timeout
        for (int taskId : allTaskIds) {
            if (NimbusUtils.isTaskDead(nimbusData, topologyId, taskId))
                deadTasks.add(taskId);
        }

        // Mark the tasks, which were assigned into the same worker with the timeout tasks, as dead task
        for (ResourceWorkerSlot worker : allWorkers) {
            Set<Integer> tasks =  worker.getTasks();
            for (Integer task : tasks) {
                if (deadTasks.contains(task)) {
                    deadTasks.addAll(tasks);
                    break;
                }
            }
        }
        return deadTasks;
    }

    /**
     * Backup topology assignment to ZK
     *
     * todo: Do we need to do backup operation every time?
     */
    public void backupAssignment(Assignment assignment, TopologyAssignEvent event) {
        String topologyId = event.getTopologyId();
        String topologyName = event.getTopologyName();
        try {

            StormClusterState zkClusterState = nimbusData.getStormClusterState();
            // one little problem, get tasks twice when assign one topology
            Map<Integer, String> tasks = Cluster.get_all_task_component(zkClusterState, topologyId, null);

            Map<String, List<Integer>> componentTasks = JStormUtils.reverse_map(tasks);

            for (Entry<String, List<Integer>> entry : componentTasks.entrySet()) {
                List<Integer> keys = entry.getValue();
                Collections.sort(keys);
            }

            AssignmentBak assignmentBak = new AssignmentBak(componentTasks, assignment);
            zkClusterState.backup_assignment(topologyName, assignmentBak);
        } catch (Exception e) {
            LOG.warn("Failed to backup " + topologyId + " assignment " + assignment, e);
        }
    }

    private void getAliveSupervsByHb(Map<String, SupervisorInfo> supervisorInfos, Map conf) {
        int currentTime = TimeUtils.current_time_secs();
        int hbTimeout = JStormUtils.parseInt(conf.get(Config.NIMBUS_SUPERVISOR_TIMEOUT_SECS), (JStormUtils.MIN_1 * 3));
        Set<String> supervisorTobeRemoved = new HashSet<>();

        for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
            SupervisorInfo supInfo = entry.getValue();
            int lastReportTime = supInfo.getTimeSecs();
            if ((currentTime - lastReportTime) > hbTimeout) {
                LOG.warn("Supervisor-" + supInfo.getHostName() + " is dead. lastReportTime=" + lastReportTime);
                supervisorTobeRemoved.add(entry.getKey());
            }
        }

        for (String name : supervisorTobeRemoved) {
            supervisorInfos.remove(name);
        }
    }
}
