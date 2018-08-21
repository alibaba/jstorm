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
package com.alibaba.jstorm.daemon.worker;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormUtils;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update current worker connections with other workers.
 * When a worker shuts down or is created, the connections need to be updated
 *
 * @author yannian/Longda
 */
public class RefreshConnections extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(RefreshConnections.class);

    private WorkerData workerData;

    @SuppressWarnings("rawtypes")
    private Map stormConf;
    private Map conf;

    private StormClusterState zkCluster;
    private String topologyId;
    private Set<Integer> outboundTasks;

    /**
     * it's actually a HashMap, but for jdk 1.8 compatibility, use the base Map interface
     */
    private Map<WorkerSlot, IConnection> nodePortToSocket;
    private IContext context;
    private ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort;
    private Integer frequency;
    private String supervisorId;
    private int taskTimeoutSecs;
    private Integer assignmentVersion = -1;

    @SuppressWarnings("rawtypes")
    public RefreshConnections(WorkerData workerData) {
        this.workerData = workerData;
        this.stormConf = workerData.getStormConf();
        this.conf = workerData.getConf();
        this.zkCluster = workerData.getZkCluster();
        this.topologyId = workerData.getTopologyId();
        this.outboundTasks = workerData.getOutboundTasks();
        this.nodePortToSocket = workerData.getNodePortToSocket();
        this.context = workerData.getContext();
        this.taskToNodePort = workerData.getTaskToNodePort();
        this.supervisorId = workerData.getSupervisorId();

        // this.endpoint_socket_lock = endpoint_socket_lock;
        frequency = JStormUtils.parseInt(stormConf.get(Config.TASK_REFRESH_POLL_SECS), 5);

        taskTimeoutSecs = JStormUtils.parseInt(stormConf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS), 10);
        taskTimeoutSecs = taskTimeoutSecs * 3;
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                Integer recordedVersion = zkCluster.assignment_version(topologyId, this);
                boolean isUpdateAssignment = !(recordedVersion != null && recordedVersion.equals(assignmentVersion));
                boolean isUpdateSupervisorTimeStamp = false;
                Long localAssignmentTS = null;
                try {
                    localAssignmentTS = StormConfig.read_supervisor_topology_timestamp(conf, topologyId);
                    isUpdateSupervisorTimeStamp = localAssignmentTS > workerData.getAssignmentTs();
                } catch (FileNotFoundException e) {
                    LOG.warn("Failed to read supervisor topology timestamp for " + topologyId +
                            " port=" + workerData.getPort(), e);
                }

                if (isUpdateAssignment || isUpdateSupervisorTimeStamp) {
                    LOG.info("update worker data due to changed assignment!!!");
                    Assignment assignment = zkCluster.assignment_info(topologyId, this);
                    if (assignment == null) {
                        String errMsg = "Failed to get assignment of " + topologyId;
                        LOG.error(errMsg);
                        // throw new RuntimeException(errMsg);
                        return;
                    }

                    // Compare the assignment timestamp of jstorm_home/data/supervisor/topo-id/timestamp
                    // with the one in workerData to check if the topology code is updated.
                    // If so, the outbound task map should be updated accordingly.
                    if (isUpdateSupervisorTimeStamp) {
                        try {
                            if (assignment.getAssignmentType() == AssignmentType.UpdateTopology) {
                                LOG.info("Get config reload request for " + topologyId);
                                // If config was updated, notify all tasks
                                List<TaskShutdownDameon> taskShutdowns = workerData.getShutdownTasks();
                                Map newConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
                                workerData.getStormConf().putAll(newConf);
                                for (TaskShutdownDameon taskSD : taskShutdowns) {
                                    taskSD.update(newConf);
                                }
                                // disable/enable metrics on the fly
                                workerData.getUpdateListener().update(newConf);

                                workerData.setAssignmentType(AssignmentType.UpdateTopology);
                            } else {
                                Set<Integer> addedTasks = getAddedTasks(assignment);
                                Set<Integer> removedTasks = getRemovedTasks(assignment);
                                Set<Integer> updatedTasks = getUpdatedTasks(assignment);

                                workerData.updateWorkerData(assignment);
                                workerData.updateKryoSerializer();

                                shutdownTasks(removedTasks);
                                createTasks(addedTasks);
                                updateTasks(updatedTasks);

                                Set<Integer> tmpOutboundTasks = Worker.worker_output_tasks(workerData);
                                if (!outboundTasks.equals(tmpOutboundTasks)) {
                                    for (int taskId : tmpOutboundTasks) {
                                        if (!outboundTasks.contains(taskId))
                                            workerData.addOutboundTaskStatusIfAbsent(taskId);
                                    }
                                    for (int taskId : workerData.getOutboundTaskStatus().keySet()) {
                                        if (!tmpOutboundTasks.contains(taskId)) {
                                            workerData.removeOutboundTaskStatus(taskId);
                                        }
                                    }
                                    workerData.setOutboundTasks(tmpOutboundTasks);
                                    outboundTasks = tmpOutboundTasks;
                                }
                                workerData.setAssignmentType(AssignmentType.Assign);
                            }

                            // If everything is OK, update the assignment TS.
                            // the tasks will update the related data.
                            if (localAssignmentTS != null)
                                workerData.setAssignmentTs(localAssignmentTS);
                        } catch (Exception e) {
                            LOG.warn("Failed to update worker data", e);
                        }
                    }

                    Set<ResourceWorkerSlot> workers = assignment.getWorkers();
                    if (workers == null) {
                        String errMsg = "Failed to get worker slots of " + topologyId;
                        LOG.error(errMsg);
                        return;
                    }

                    workerData.updateWorkerToResource(workers);

                    Map<Integer, WorkerSlot> taskNodePortTmp = new HashMap<>();
                    Map<String, String> node = assignment.getNodeHost();

                    // only reserve outboundTasks
                    Set<ResourceWorkerSlot> needConnections = new HashSet<>();
                    Set<Integer> localTasks = new HashSet<>();
                    Set<Integer> localNodeTasks = new HashSet<>();

                    if (outboundTasks != null) {
                        for (ResourceWorkerSlot worker : workers) {
                            if (supervisorId.equals(worker.getNodeId()))
                                localNodeTasks.addAll(worker.getTasks());
                            if (supervisorId.equals(worker.getNodeId()) && worker.getPort() == workerData.getPort())
                                localTasks.addAll(worker.getTasks());
                            for (Integer id : worker.getTasks()) {
                                taskNodePortTmp.put(id, worker);
                                if (outboundTasks.contains(id)) {
                                    needConnections.add(worker);
                                }
                            }
                        }
                    }
                    taskToNodePort.putAll(taskNodePortTmp);
                    //workerData.setLocalTasks(localTasks);
                    workerData.setLocalNodeTasks(localNodeTasks);

                    // get which connection need to be remove or add
                    Set<WorkerSlot> currentConnections = nodePortToSocket.keySet();
                    Set<ResourceWorkerSlot> newConnections = new HashSet<>();
                    Set<WorkerSlot> removeConnections = new HashSet<>();

                    for (ResourceWorkerSlot nodePort : needConnections) {
                        if (!currentConnections.contains(nodePort)) {
                            newConnections.add(nodePort);
                        }
                    }

                    for (WorkerSlot node_port : currentConnections) {
                        if (!needConnections.contains(node_port)) {
                            removeConnections.add(node_port);
                        }
                    }

                    // create new connection
                    for (ResourceWorkerSlot nodePort : newConnections) {
                        String host = node.get(nodePort.getNodeId());
                        int port = nodePort.getPort();
                        IConnection conn = context.connect(topologyId, host, port, workerData.getTaskIds(), nodePort.getTasks());
                        nodePortToSocket.put(nodePort, conn);
                        LOG.info("Add connection to " + nodePort);
                    }

                    // close useless connection
                    for (WorkerSlot node_port : removeConnections) {
                        LOG.info("Remove connection to " + node_port);
                        nodePortToSocket.remove(node_port).close();
                    }
                }

                // check the status of connections to all outbound tasks
                boolean allConnectionReady = true;
                for (Integer taskId : outboundTasks) {
                    boolean isConnected = isOutTaskConnected(taskId);
                    if (!isConnected)
                        allConnectionReady = isConnected;
                    workerData.updateOutboundTaskStatus(taskId, isConnected);
                }
                if (allConnectionReady) {
                    workerData.getWorkerInitConnectionStatus().getAndSet(allConnectionReady);
                }

                if (recordedVersion != null)
                    assignmentVersion = recordedVersion;

            }
        } catch (Exception e) {
            LOG.error("Failed to refresh worker connections", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getResult() {
        return frequency;
    }

    private Set<Integer> getAddedTasks(Assignment assignment) {
        Set<Integer> ret = new HashSet<>();
        try {
            Set<Integer> taskIds = assignment.getCurrentWorkerTasks(workerData.getSupervisorId(), workerData.getPort());
            for (Integer taskId : taskIds) {
                if (!(workerData.getTaskIds().contains(taskId)))
                    ret.add(taskId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get added task list for" + workerData.getTopologyId());
        }
        return ret;
    }

    private Set<Integer> getRemovedTasks(Assignment assignment) {
        Set<Integer> ret = new HashSet<>();
        try {
            Set<Integer> taskIds = assignment.getCurrentWorkerTasks(workerData.getSupervisorId(), workerData.getPort());
            for (Integer taskId : workerData.getTaskIds()) {
                if (!(taskIds.contains(taskId)))
                    ret.add(taskId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get removed task list for" + workerData.getTopologyId());
        }
        return ret;
    }

    private Set<Integer> getUpdatedTasks(Assignment assignment) {
        Set<Integer> ret = new HashSet<>();
        try {
            Set<Integer> taskIds = assignment.getCurrentWorkerTasks(workerData.getSupervisorId(), workerData.getPort());
            for (Integer taskId : taskIds) {
                if ((workerData.getTaskIds().contains(taskId)))
                    ret.add(taskId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get updated task list for" + workerData.getTopologyId());
        }
        return ret;
    }

    private void createTasks(Set<Integer> tasks) {
        if (tasks == null)
            return;

        for (Integer taskId : tasks) {
            try {
                TaskShutdownDameon shutdown = Task.mk_task(workerData, taskId);
                workerData.addShutdownTask(shutdown);
            } catch (Exception e) {
                LOG.error("Failed to create task-" + taskId, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void shutdownTasks(Set<Integer> tasks) {
        if (tasks == null)
            return;

        List<TaskShutdownDameon> shutdowns = workerData.getShutdownDaemonbyTaskIds(tasks);
        List<Future> futures = new ArrayList<>();
        for (ShutdownableDameon task : shutdowns) {
            Future<?> future = workerData.getFlusherPool().submit(task);
            futures.add(future);
        }
        // To be assure all tasks are closed rightly
        for (Future future : futures) {
            if (future != null) {
                try {
                    future.get();
                } catch (Exception ex) {
                    LOG.error("Failed to shutdown task {}", ex);
                }
            }
        }
    }

    private void updateTasks(Set<Integer> tasks) {
        if (tasks == null)
            return;

        List<TaskShutdownDameon> shutdowns = workerData.getShutdownDaemonbyTaskIds(tasks);
        for (TaskShutdownDameon shutdown : shutdowns) {
            try {
                shutdown.getTask().updateTaskData();
            } catch (Exception e) {
                LOG.error("Failed to update task-" + shutdown.getTaskId(), e);
            }
        }
    }

    private boolean isOutTaskConnected(int taskId) {
        boolean ret = false;

        if (workerData.getInnerTaskTransfer().get(taskId) != null) {
            // Connections to inner tasks should be done after initialization.
            // So return true here for all inner tasks.
            ret = true;
        } else {
            WorkerSlot slot = taskToNodePort.get(taskId);
            if (slot != null) {
                IConnection connection = nodePortToSocket.get(slot);
                if (connection != null) {
                    ret = connection.available(taskId);
                }
            }
        }

        return ret;
    }
}
