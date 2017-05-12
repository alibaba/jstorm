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
package com.alibaba.jstorm.schedule;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.TaskDeadEvent;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeFormat;

import backtype.storm.Config;
import backtype.storm.generated.TaskHeartbeat;
import backtype.storm.generated.TopologyTaskHbInfo;

/**
 * Scan all task's heartbeat, if a task isn't alive, do NimbusUtils.transition(monitor)
 *
 * @author Longda
 */
public class MonitorRunnable implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(MonitorRunnable.class);

    private NimbusData data;

    public MonitorRunnable(NimbusData data) {
        this.data = data;
    }

    /**
     * Todo: when one topology is being reassigned, the topology should skip check
     */
    @Override
    public void run() {
        StormClusterState clusterState = data.getStormClusterState();

        try {
            // Note: need first check Assignments
            List<String> activeTopologies = clusterState.assignments(null);

            if (activeTopologies == null) {
                LOG.info("Failed to get active topologies");
                return;
            }

            for (String topologyId : activeTopologies) {
                if (clusterState.storm_base(topologyId, null) == null) {
                    continue;
                }

                LOG.debug("Check tasks of topology " + topologyId);

                // Note that we don't check /ZK-dir/taskbeats/topologyId to get task ids
                Set<Integer> taskIds = clusterState.task_ids(topologyId);
                if (taskIds == null) {
                    LOG.info("Failed to get task ids of " + topologyId);
                    continue;
                }
                Assignment assignment = clusterState.assignment_info(topologyId, null);

                Set<Integer> deadTasks = new HashSet<>();
                boolean needReassign = false;
                for (Integer task : taskIds) {
                    boolean isTaskDead = NimbusUtils.isTaskDead(data, topologyId, task);
                    if (isTaskDead) {
                        deadTasks.add(task);
                        needReassign = true;
                    }
                }

                TopologyTaskHbInfo topologyHbInfo = data.getTasksHeartbeat().get(topologyId);
                if (needReassign) {
                    if (topologyHbInfo != null) {
                        int topologyMasterId = topologyHbInfo.get_topologyMasterId();
                        if (deadTasks.contains(topologyMasterId)) {
                            deadTasks.clear();
                            if (assignment != null) {
                                ResourceWorkerSlot resource = assignment.getWorkerByTaskId(topologyMasterId);
                                if (resource != null)
                                    deadTasks.addAll(resource.getTasks());
                                else
                                    deadTasks.add(topologyMasterId);
                            }
                        } else {
                            Map<Integer, TaskHeartbeat> taskHbs = topologyHbInfo.get_taskHbs();
                            int launchTime = JStormUtils.parseInt(data.getConf().get(Config.NIMBUS_TASK_LAUNCH_SECS));
                            if (taskHbs == null || taskHbs.get(topologyMasterId) == null ||
                                    taskHbs.get(topologyMasterId).get_uptime() < launchTime) {
                                /*try {
                                    clusterState.topology_heartbeat(topologyId, topologyHbInfo);
                                } catch (Exception e) {
                                    LOG.error("Failed to update task heartbeat info to ZK for " + topologyId, e);
                                }*/
                                return;
                            }
                        }
                        Map<Integer, ResourceWorkerSlot> deadTaskWorkers = new HashMap<>();
                        for (Integer task : deadTasks) {
                            LOG.info("Found " + topologyId + ", taskId:" + task + " is dead");

                            ResourceWorkerSlot resource = null;
                            if (assignment != null)
                                resource = assignment.getWorkerByTaskId(task);
                            if (resource != null) {
                                deadTaskWorkers.put(task, resource);
                            }
                        }
                        Map<ResourceWorkerSlot, List<Integer>> workersDeadTasks = JStormUtils.reverse_map(deadTaskWorkers);
                        for (Map.Entry<ResourceWorkerSlot, List<Integer>> entry : workersDeadTasks.entrySet()) {
                            ResourceWorkerSlot resource = entry.getKey();
                            //we only report one task
                            for (Integer task : entry.getValue()) {
                                Date now = new Date();
                                String nowStr = TimeFormat.getSecond(now);
                                String errorInfo = "Task-" + entry.getValue().toString() + " is dead on " +
                                        resource.getHostname() + ":" + resource.getPort() + ", " + nowStr;
                                LOG.info(errorInfo);
                                clusterState.report_task_error(topologyId, task, errorInfo, ErrorConstants.ERROR,
                                        ErrorConstants.CODE_TASK_DEAD, ErrorConstants.DURATION_SECS_TASK_DEAD);
                                break;
                            }
                        }
                        if (deadTaskWorkers.size() > 0) {
                            // notify jstorm monitor
                            TaskDeadEvent.pushEvent(topologyId, deadTaskWorkers);
                        }
                    }
                    NimbusUtils.transition(data, topologyId, false, StatusType.monitor);
                }

                if (topologyHbInfo != null) {
                    try {
                        clusterState.topology_heartbeat(topologyId, topologyHbInfo);
                    } catch (Exception e) {
                        LOG.error("Failed to update task heartbeat info to ZK for " + topologyId, e);
                    }
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
