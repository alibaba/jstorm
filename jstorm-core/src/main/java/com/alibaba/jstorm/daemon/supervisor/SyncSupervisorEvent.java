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
package com.alibaba.jstorm.daemon.supervisor;

import backtype.storm.utils.LocalState;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.LocalAssignment;
import com.alibaba.jstorm.event.EventManager;
import com.alibaba.jstorm.event.EventManagerZkPusher;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * supervisor SynchronizeSupervisor workflow
 * 1. writer local assignment to LocalState
 * 2. download new assignments of topologies
 * 3. remove useless topologies
 * 4. push a SyncProcessEvent to SyncProcessEvent's EventManager
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
class SyncSupervisorEvent extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(SyncSupervisorEvent.class);

    // private Supervisor supervisor;
    private String supervisorId;
    private EventManager syncSupEventManager;
    private StormClusterState stormClusterState;
    private LocalState localState;
    private Map<Object, Object> conf;
    private SyncProcessEvent syncProcesses;
    private int lastTime;
    private Heartbeat heartbeat;

    @SuppressWarnings("unchecked")
    public SyncSupervisorEvent(String supervisorId, Map conf, EventManager syncSupEventManager,
                               StormClusterState stormClusterState, LocalState localState,
                               SyncProcessEvent syncProcesses, Heartbeat heartbeat) {
        this.syncProcesses = syncProcesses;
        this.syncSupEventManager = syncSupEventManager;
        this.stormClusterState = stormClusterState;
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.localState = localState;
        this.heartbeat = heartbeat;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        LOG.debug("Synchronizing supervisor, interval (sec): " + TimeUtils.time_delta(lastTime));
        lastTime = TimeUtils.current_time_secs();
        // make sure that the status is the same for each execution of syncsupervisor
        HealthStatus healthStatus = heartbeat.getHealthStatus();
        try {
            RunnableCallback syncCallback = new EventManagerZkPusher(this, syncSupEventManager);

            Map<String, Integer> assignmentVersion = (Map<String, Integer>) localState.get(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION);
            if (assignmentVersion == null) {
                assignmentVersion = new HashMap<>();
            }
            Map<String, Assignment> assignments = (Map<String, Assignment>) localState.get(Common.LS_LOCAl_ZK_ASSIGNMENTS);
            if (assignments == null) {
                assignments = new HashMap<>();
            }
            LOG.debug("get local assignments  " + assignments);
            LOG.debug("get local assignments version " + assignmentVersion);

            /**
             * Step 1: get all assignments and add assignment watchers for /ZK-dir/assignment
             */
            if (healthStatus.isMoreSeriousThan(HealthStatus.ERROR)) {
                // if status is panic or error, clear all assignments and kill all workers
                assignmentVersion.clear();
                assignments.clear();
                LOG.warn("Supervisor machine check status: " + healthStatus + ", killing all workers.");
            } else {
                getAllAssignments(assignmentVersion, assignments, syncCallback);
            }
            LOG.debug("Get all assignments " + assignments);

            /**
             * Step 2: get topology id list from STORM-LOCAL-DIR/supervisor/stormdist/
             */
            List<String> downloadedTopologyIds = StormConfig.get_supervisor_toplogy_list(conf);
            LOG.debug("Downloaded storm ids: " + downloadedTopologyIds);

            /**
             * Step 3: get <port,LocalAssignments> from ZK local node's assignment
             */
            Map<Integer, LocalAssignment> zkAssignment = getLocalAssign(stormClusterState, supervisorId, assignments);

            Map<Integer, LocalAssignment> localAssignment;

            /**
             * Step 4: writer local assignment to LocalState
             */
            try {
                LOG.debug("Writing local assignment " + zkAssignment);
                localAssignment = (Map<Integer, LocalAssignment>) localState.get(Common.LS_LOCAL_ASSIGNMENTS);
                if (localAssignment == null) {
                    localAssignment = new HashMap<>();
                }
                localState.put(Common.LS_LOCAL_ASSIGNMENTS, zkAssignment);
            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ASSIGNMENTS " + zkAssignment + " to localState failed");
                throw e;
            }

            /**
             * Step 5: get reloaded topologies
             */
            Set<String> updateTopologies = getUpdateTopologies(localAssignment, zkAssignment, assignments);
            Set<String> reDownloadTopologies = getNeedReDownloadTopologies(localAssignment);
            if (reDownloadTopologies != null) {
                updateTopologies.addAll(reDownloadTopologies);
            }

            /**
             * get upgrade topology ports
             */
            Map<String, Set<Pair<String, Integer>>> upgradeTopologyPorts = getUpgradeTopologies(
                    stormClusterState, localAssignment, zkAssignment);
            if (upgradeTopologyPorts.size() > 0) {
                LOG.info("upgrade topology ports:{}", upgradeTopologyPorts);
                updateTopologies.addAll(upgradeTopologyPorts.keySet());
            }

            /**
             * Step 6: download code from ZK
             */
            Map<String, String> topologyCodes = getTopologyCodeLocations(assignments, supervisorId);
            // downloadFailedTopologyIds which can't finished download binary from nimbus
            Set<String> downloadFailedTopologyIds = new HashSet<>();
            downloadTopology(topologyCodes, downloadedTopologyIds,
                    updateTopologies, assignments, downloadFailedTopologyIds);

            /**
             * Step 7: remove any downloaded useless topology
             */
            removeUselessTopology(topologyCodes, downloadedTopologyIds);
            /**
             * Step 8: push syncProcesses Event
             */
            // processEventManager.add(syncProcesses);
            syncProcesses.run(zkAssignment, downloadFailedTopologyIds, upgradeTopologyPorts);

            // set the trigger to update heartbeat of supervisor
            heartbeat.updateHbTrigger(true);

            try {
                // update localState
                localState.put(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION, assignmentVersion);
                localState.put(Common.LS_LOCAl_ZK_ASSIGNMENTS, assignments);
            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ZK_ASSIGNMENT_VERSION & LS_LOCAl_ZK_ASSIGNMENTS to localState failed");
                throw e;
            }
        } catch (Exception e) {
            LOG.error("Failed to init SyncSupervisorEvent", e);
            // throw new RuntimeException(e);
        }
        if (healthStatus.isMoreSeriousThan(HealthStatus.PANIC)) {
            // if status is panic, kill supervisor;
            JStormUtils.halt_process(0, "Supervisor machine check status: Panic! !!!!shutdown!!!!");
        }

    }

    /**
     * download code with two cluster modes: local and distributed
     */
    private void downloadStormCode(Map conf, String topologyId, String masterCodeDir) throws IOException, TException {
        String clusterMode = StormConfig.cluster_mode(conf);

        if (clusterMode.endsWith("distributed")) {
            BlobStoreUtils.downloadDistributeStormCode(conf, topologyId, masterCodeDir);
        } else if (clusterMode.endsWith("local")) {
            BlobStoreUtils.downloadLocalStormCode(conf, topologyId, masterCodeDir);
        }
    }


    /**
     * a port must be assigned to a topology
     *
     * @return map: [port,LocalAssignment]
     */
    private Map<Integer, LocalAssignment> getLocalAssign(StormClusterState stormClusterState, String supervisorId,
                                                         Map<String, Assignment> assignments) throws Exception {
        Map<Integer, LocalAssignment> portToAssignment = new HashMap<>();
        for (Entry<String, Assignment> assignEntry : assignments.entrySet()) {
            String topologyId = assignEntry.getKey();
            Assignment assignment = assignEntry.getValue();

            Map<Integer, LocalAssignment> portTasks = readMyTasks(stormClusterState, topologyId, supervisorId, assignment);
            if (portTasks == null) {
                continue;
            }

            // a port must be assigned to one assignment
            for (Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {
                Integer port = entry.getKey();
                LocalAssignment la = entry.getValue();
                if (!portToAssignment.containsKey(port)) {
                    portToAssignment.put(port, la);
                } else {
                    throw new RuntimeException("Should not have multiple topologies assigned to one port");
                }
            }
        }

        return portToAssignment;
    }

    /**
     * get local node's tasks
     *
     * @return Map: [port, LocalAssignment]
     */
    @SuppressWarnings("unused")
    private Map<Integer, LocalAssignment> readMyTasks(StormClusterState stormClusterState, String topologyId,
                                                      String supervisorId, Assignment assignmentInfo) throws Exception {
        Map<Integer, LocalAssignment> portTasks = new HashMap<>();

        Set<ResourceWorkerSlot> workers = assignmentInfo.getWorkers();
        if (workers == null) {
            LOG.error("No worker found for assignment {}!", assignmentInfo);
            return portTasks;
        }

        for (ResourceWorkerSlot worker : workers) {
            if (!supervisorId.equals(worker.getNodeId())) {
                continue;
            }
            portTasks.put(worker.getPort(), new LocalAssignment(
                    topologyId, worker.getTasks(), Common.topologyIdToName(topologyId), worker.getMemSize(),
                    worker.getCpu(), worker.getJvm(), assignmentInfo.getTimeStamp()));
        }

        return portTasks;
    }

    /**
     * get master code dir for each topology
     *
     * @return Map: [topologyId, master-code-dir] from zookeeper
     */
    public static Map<String, String> getTopologyCodeLocations(
            Map<String, Assignment> assignments, String supervisorId) throws Exception {
        Map<String, String> rtn = new HashMap<>();
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            String topologyId = entry.getKey();
            Assignment assignmentInfo = entry.getValue();

            Set<ResourceWorkerSlot> workers = assignmentInfo.getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                String node = worker.getNodeId();
                if (supervisorId.equals(node)) {
                    rtn.put(topologyId, assignmentInfo.getMasterCodeDir());
                    break;
                }
            }

        }
        return rtn;
    }

    public void downloadTopology(Map<String, String> topologyCodes, List<String> downloadedTopologyIds,
                                 Set<String> updateTopologies, Map<String, Assignment> assignments,
                                 Set<String> downloadFailedTopologyIds) throws Exception {
        Set<String> downloadTopologies = new HashSet<>();
        for (Entry<String, String> entry : topologyCodes.entrySet()) {
            String topologyId = entry.getKey();
            String masterCodeDir = entry.getValue();
            if (!downloadedTopologyIds.contains(topologyId) || updateTopologies.contains(topologyId)) {
                LOG.info("Downloading code for storm id " + topologyId + " from " + masterCodeDir);
                int retry = 0;
                while (retry < 3) {
                    try {
                        downloadStormCode(conf, topologyId, masterCodeDir);
                        // Update assignment timeStamp
                        StormConfig.write_supervisor_topology_timestamp(
                                conf, topologyId, assignments.get(topologyId).getTimeStamp());
                        break;
                    } catch (IOException | TException e) {
                        LOG.error(e + " downloadStormCode failed, topologyId:" + topologyId +
                                ", masterCodeDir:" + masterCodeDir);
                    }
                    retry++;
                }
                if (retry < 3) {
                    LOG.info("Finished downloading code for storm id " + topologyId + " from " + masterCodeDir);
                    downloadTopologies.add(topologyId);
                } else {
                    LOG.error("Failed to download code for storm id " + topologyId + " from " + masterCodeDir);
                    downloadFailedTopologyIds.add(topologyId);
                }
            }
        }
        // clear directory of topologyId is dangerous , so it only clear the topologyId which
        // isn't contained by downloadedTopologyIds
        for (String topologyId : downloadFailedTopologyIds) {
            if (!downloadedTopologyIds.contains(topologyId)) {
                try {
                    String stormroot = StormConfig.supervisor_stormdist_root(conf, topologyId);
                    File destDir = new File(stormroot);
                    FileUtils.deleteQuietly(destDir);
                } catch (Exception e) {
                    LOG.error("Failed to clear directory about storm id " + topologyId + " on supervisor ");
                }
            }
        }

        updateTaskCleanupTimeout(downloadTopologies);
    }

    public void removeUselessTopology(Map<String, String> topologyCodes, List<String> downloadedTopologyIds) {
        for (String topologyId : downloadedTopologyIds) {
            if (!topologyCodes.containsKey(topologyId)) {
                LOG.info("Removing code for storm id " + topologyId);
                String path = null;
                try {
                    path = StormConfig.supervisor_stormdist_root(conf, topologyId);
                    PathUtils.rmr(path);
                } catch (IOException e) {
                    String errMsg = "rmr the path:" + path + "failed\n";
                    LOG.error(errMsg, e);
                }
            }
        }
    }

    private Set<String> getUpdateTopologies(Map<Integer, LocalAssignment> localAssignments,
                                            Map<Integer, LocalAssignment> zkAssignments,
                                            Map<String, Assignment> assignments) {
        Set<String> ret = new HashSet<>();
        if (localAssignments != null && zkAssignments != null) {
            for (Entry<Integer, LocalAssignment> entry : localAssignments.entrySet()) {
                Integer port = entry.getKey();
                LocalAssignment localAssignment = entry.getValue();
                LocalAssignment zkAssignment = zkAssignments.get(port);
                if (localAssignment == null || zkAssignment == null)
                    continue;

                Assignment assignment = assignments.get(localAssignment.getTopologyId());
                if (localAssignment.getTopologyId().equals(zkAssignment.getTopologyId()) && assignment != null
                        && assignment.isTopologyChange(localAssignment.getTimeStamp())) {
                    if (ret.add(localAssignment.getTopologyId())) {
                        LOG.info("Topology " + localAssignment.getTopologyId() +
                                " has been updated. LocalTs=" + localAssignment.getTimeStamp() + ", ZkTs="
                                + zkAssignment.getTimeStamp());
                    }
                }
            }
        }

        return ret;
    }

    private Map<String, Set<Pair<String, Integer>>> getUpgradeTopologies(StormClusterState stormClusterState,
                                                                         Map<Integer, LocalAssignment> localAssignments,
                                                                         Map<Integer, LocalAssignment> zkAssignments) {
        SupervisorInfo supervisorInfo = heartbeat.getSupervisorInfo();
        Map<String, Set<Pair<String, Integer>>> ret = new HashMap<>();

        try {
            Set<String> upgradingTopologies = Sets.newHashSet(stormClusterState.get_upgrading_topologies());
            for (String topologyId : upgradingTopologies) {
                List<String> upgradingWorkers = stormClusterState.get_upgrading_workers(topologyId);
                for (String worker : upgradingWorkers) {
                    String[] hostPort = worker.split(":");
                    String host = hostPort[0];
                    Integer port = Integer.valueOf(hostPort[1]);
                    if (host.equals(supervisorInfo.getHostName()) &&
                            supervisorInfo.getWorkerPorts().contains(port) &&
                            localAssignments.containsKey(port) && zkAssignments.containsKey(port)) {
                        Set<Pair<String, Integer>> ports = ret.get(topologyId);
                        if (ports == null) {
                            ports = new HashSet<>();
                        }
                        ports.add(new Pair<>(host, port));

                        ret.put(topologyId, ports);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to get upgrading topologies", ex);
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private Set<String> getNeedReDownloadTopologies(Map<Integer, LocalAssignment> localAssignment) {
        Set<String> reDownloadTopologies = syncProcesses.getTopologyIdNeedDownload().getAndSet(null);
        if (reDownloadTopologies == null || reDownloadTopologies.size() == 0)
            return null;
        Set<String> needRemoveTopologies = new HashSet<>();
        Map<Integer, String> portToStartWorkerId = syncProcesses.getPortToWorkerId();
        for (Entry<Integer, LocalAssignment> entry : localAssignment.entrySet()) {
            if (portToStartWorkerId.containsKey(entry.getKey()))
                needRemoveTopologies.add(entry.getValue().getTopologyId());
        }
        LOG.debug("workers are starting on these topologies, delay downloading topology binary: " +
                needRemoveTopologies);
        reDownloadTopologies.removeAll(needRemoveTopologies);
        if (reDownloadTopologies.size() > 0)
            LOG.info("Following topologies are going to re-download the jars, " + reDownloadTopologies);
        return reDownloadTopologies;
    }

    @SuppressWarnings("unchecked")
    private void updateTaskCleanupTimeout(Set<String> topologies) {
        Map topologyConf = null;
        Map<String, Integer> taskCleanupTimeouts = new HashMap<>();
        for (String topologyId : topologies) {
            try {
                topologyConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
            } catch (IOException e) {
                LOG.info("Failed to read conf for " + topologyId);
            }

            Integer cleanupTimeout = null;
            if (topologyConf != null) {
                cleanupTimeout = JStormUtils.parseInt(topologyConf.get(ConfigExtension.TASK_CLEANUP_TIMEOUT_SEC));
            }

            if (cleanupTimeout == null) {
                cleanupTimeout = ConfigExtension.getTaskCleanupTimeoutSec(conf);
            }

            taskCleanupTimeouts.put(topologyId, cleanupTimeout);
        }

        Map<String, Integer> localTaskCleanupTimeouts = null;
        try {
            localTaskCleanupTimeouts = (Map<String, Integer>) localState.get(Common.LS_TASK_CLEANUP_TIMEOUT);
        } catch (IOException e) {
            LOG.error("Failed to read local task cleanup timeout map", e);
        }

        if (localTaskCleanupTimeouts == null)
            localTaskCleanupTimeouts = taskCleanupTimeouts;
        else
            localTaskCleanupTimeouts.putAll(taskCleanupTimeouts);

        try {
            localState.put(Common.LS_TASK_CLEANUP_TIMEOUT, localTaskCleanupTimeouts);
        } catch (IOException e) {
            LOG.error("Failed to write local task cleanup timeout map", e);
        }
    }

    private void getAllAssignments(Map<String, Integer> assignmentVersion,
                                   Map<String, Assignment> localZkAssignments,
                                   RunnableCallback callback) throws Exception {
        Map<String, Assignment> ret = new HashMap<>();
        Map<String, Integer> updateAssignmentVersion = new HashMap<>();

        // get /assignments {topologyId}
        List<String> assignments = stormClusterState.assignments(callback);
        if (assignments == null) {
            assignmentVersion.clear();
            localZkAssignments.clear();
            LOG.debug("No assignment in ZK");
            return;
        }

        for (String topologyId : assignments) {
            Integer zkVersion = stormClusterState.assignment_version(topologyId, callback);
            LOG.debug(topologyId + "'s assignment version of zk is :" + zkVersion);
            Integer recordedVersion = assignmentVersion.get(topologyId);
            LOG.debug(topologyId + "'s assignment version of local is :" + recordedVersion);

            Assignment assignment = null;
            if (recordedVersion != null && zkVersion != null && recordedVersion.equals(zkVersion)) {
                assignment = localZkAssignments.get(topologyId);
            }
            //because the first version is 0
            if (assignment == null) {
                assignment = stormClusterState.assignment_info(topologyId, callback);
            }
            if (assignment == null) {
                LOG.error("Failed to get assignment of " + topologyId + " from ZK");
                continue;
            }
            updateAssignmentVersion.put(topologyId, zkVersion);
            ret.put(topologyId, assignment);
        }
        assignmentVersion.clear();
        assignmentVersion.putAll(updateAssignmentVersion);
        localZkAssignments.clear();
        localZkAssignments.putAll(ret);
    }

}
