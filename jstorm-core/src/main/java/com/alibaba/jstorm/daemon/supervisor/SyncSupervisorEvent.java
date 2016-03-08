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
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

/**
 * supervisor SynchronizeSupervisor workflow (1) writer local assignment to LocalState (2) download new Assignment's topology (3) remove useless Topology (4)
 * push one SyncProcessEvent to SyncProcessEvent's EventManager
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


    /**
     * @param conf
     * @param syncSupEventManager
     * @param stormClusterState
     * @param supervisorId
     * @param localState
     * @param syncProcesses
     */
    public SyncSupervisorEvent(String supervisorId, Map conf, EventManager syncSupEventManager,
            StormClusterState stormClusterState, LocalState localState, SyncProcessEvent syncProcesses, Heartbeat heartbeat) {

        this.syncProcesses = syncProcesses;
        this.syncSupEventManager = syncSupEventManager;
        this.stormClusterState = stormClusterState;
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.localState = localState;
        this.heartbeat = heartbeat;
    }

    @Override
    public void run() {
        LOG.debug("Synchronizing supervisor, interval seconds:" + TimeUtils.time_delta(lastTime));
        lastTime = TimeUtils.current_time_secs();
        //In order to ensure that the status is the same for each execution of syncsupervisor
        MachineCheckStatus checkStatus = new MachineCheckStatus();
        checkStatus.SetType(heartbeat.getCheckStatus().getType());

        try {
            RunnableCallback syncCallback = new EventManagerZkPusher(this, syncSupEventManager);

            Map<String, Integer> assignmentVersion = (Map<String, Integer>) localState.get(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION);
            if (assignmentVersion == null) {
                assignmentVersion = new HashMap<String, Integer>();
            }
            Map<String, Assignment> assignments = (Map<String, Assignment>) localState.get(Common.LS_LOCAl_ZK_ASSIGNMENTS);
            if (assignments == null) {
                assignments = new HashMap<String, Assignment>();
            }
            LOG.debug("get local assignments  " + assignments);
            LOG.debug("get local assignments version " + assignmentVersion);

            /**
             * Step 1: get all assignments and register /ZK-dir/assignment and every assignment watch
             *
             */

            if (checkStatus.getType().equals(MachineCheckStatus.StatusType.panic) || checkStatus.getType().equals(MachineCheckStatus.StatusType.error)){
                // if statuts is pannic or error, it will clear all assignments and kill all workers;
                assignmentVersion.clear();
                assignments.clear();
                LOG.warn("Supervisor Machine Check Status :" + checkStatus.getType() +", so kill all workers.");
            } else {
                getAllAssignments(assignmentVersion, assignments, syncCallback);
            }
            LOG.debug("Get all assignments " + assignments);

            /**
             * Step 2: get topologyIds list from STORM-LOCAL-DIR/supervisor/stormdist/
             */
            List<String> downloadedTopologyIds = StormConfig.get_supervisor_toplogy_list(conf);
            LOG.debug("Downloaded storm ids: " + downloadedTopologyIds);

            /**
             * Step 3: get <port,LocalAssignments> from ZK local node's assignment
             */
            Map<Integer, LocalAssignment> zkAssignment;
            zkAssignment = getLocalAssign(stormClusterState, supervisorId, assignments);

            Map<Integer, LocalAssignment> localAssignment;

            /**
             * Step 4: writer local assignment to LocalState
             */
            try {
                LOG.debug("Writing local assignment " + zkAssignment);
                localAssignment = (Map<Integer, LocalAssignment>) localState.get(Common.LS_LOCAL_ASSIGNMENTS);
                if (localAssignment == null) {
                    localAssignment = new HashMap<Integer, LocalAssignment>();
                }
                localState.put(Common.LS_LOCAL_ASSIGNMENTS, zkAssignment);

            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ASSIGNMENTS " + zkAssignment + " of localState failed");
                throw e;
            }

            /**
             * Step 5: get reloaded topologys
             */
            Set<String> updateTopologys;
            updateTopologys = getUpdateTopologys(localAssignment, zkAssignment, assignments);
            Set<String> reDownloadTopologys = getNeedReDownloadTopologys(localAssignment);
            if (reDownloadTopologys != null) {
                updateTopologys.addAll(reDownloadTopologys);
            }

            /**
             * Step 6: download code from ZK
             */
            Map<String, String> topologyCodes = getTopologyCodeLocations(assignments, supervisorId);
            // downloadFailedTopologyIds which can't finished download binary from nimbus
            Set<String> downloadFailedTopologyIds = new HashSet<String>();

            downloadTopology(topologyCodes, downloadedTopologyIds, updateTopologys, assignments, downloadFailedTopologyIds);

            /**
             * Step 7: remove any downloaded useless topology
             */
            removeUselessTopology(topologyCodes, downloadedTopologyIds);
            /**
             * Step 7: push syncProcesses Event
             */
            // processEventManager.add(syncProcesses);
            syncProcesses.run(zkAssignment, downloadFailedTopologyIds);

            // If everything is OK, set the trigger to update heartbeat of
            // supervisor
            heartbeat.updateHbTrigger(true);

            try {
                // update localState
                localState.put(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION, assignmentVersion);
                localState.put(Common.LS_LOCAl_ZK_ASSIGNMENTS, assignments);

            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ZK_ASSIGNMENT_VERSION&&LS_LOCAl_ZK_ASSIGNMENTS  failed");
                throw e;
            }
        } catch (Exception e) {
            LOG.error("Failed to Sync Supervisor", e);
            // throw new RuntimeException(e);
        }
        if (checkStatus.getType().equals(MachineCheckStatus.StatusType.panic)){
            // if statuts is pannic, it will kill supervisor;
            JStormUtils.halt_process(0, "Supervisor Machine Check Status : Panic , !!!!shutdown!!!!");
        }

    }

    /**
     * download code ; two cluster mode: local and distributed
     * 
     * @param conf
     * @param topologyId
     * @param masterCodeDir
     * @throws IOException
     */
    private void downloadStormCode(Map conf, String topologyId, String masterCodeDir) throws IOException, TException {
        String clusterMode = StormConfig.cluster_mode(conf);

        if (clusterMode.endsWith("distributed")) {
            downloadDistributeStormCode(conf, topologyId, masterCodeDir);
        } else if (clusterMode.endsWith("local")) {
            downloadLocalStormCode(conf, topologyId, masterCodeDir);

        }
    }

    private void downloadLocalStormCode(Map conf, String topologyId, String masterCodeDir) throws IOException, TException {

        // STORM-LOCAL-DIR/supervisor/stormdist/storm-id
        String stormroot = StormConfig.supervisor_stormdist_root(conf, topologyId);

        FileUtils.copyDirectory(new File(masterCodeDir), new File(stormroot));

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();

        String resourcesJar = resourcesJar();

        URL url = classloader.getResource(StormConfig.RESOURCES_SUBDIR);

        String targetDir = stormroot + '/' + StormConfig.RESOURCES_SUBDIR;

        if (resourcesJar != null) {

            LOG.info("Extracting resources from jar at " + resourcesJar + " to " + targetDir);

            JStormUtils.extractDirFromJar(resourcesJar, StormConfig.RESOURCES_SUBDIR, stormroot);// extract dir
            // from jar;;
            // util.clj
        } else if (url != null) {

            LOG.info("Copying resources at " + url.toString() + " to " + targetDir);

            FileUtils.copyDirectory(new File(url.getFile()), (new File(targetDir)));

        }
    }

    /**
     * Don't need synchronize, due to EventManager will execute serially
     * 
     * @param conf
     * @param topologyId
     * @param masterCodeDir
     * @throws IOException
     * @throws TException
     */
    private void downloadDistributeStormCode(Map conf, String topologyId, String masterCodeDir) throws IOException, TException {

        // STORM_LOCAL_DIR/supervisor/tmp/(UUID)
        String tmproot = StormConfig.supervisorTmpDir(conf) + File.separator + UUID.randomUUID().toString();

        // STORM_LOCAL_DIR/supervisor/stormdist/topologyId
        String stormroot = StormConfig.supervisor_stormdist_root(conf, topologyId);

        JStormServerUtils.downloadCodeFromMaster(conf, tmproot, masterCodeDir, topologyId, true);

        // tmproot/stormjar.jar
        String localFileJarTmp = StormConfig.stormjar_path(tmproot);

        // extract dir from jar
        JStormUtils.extractDirFromJar(localFileJarTmp, StormConfig.RESOURCES_SUBDIR, tmproot);

        File srcDir = new File(tmproot);
        File destDir = new File(stormroot);
        try {
            FileUtils.moveDirectory(srcDir, destDir);
        } catch (FileExistsException e) {
            FileUtils.copyDirectory(srcDir, destDir);
            FileUtils.deleteQuietly(srcDir);
        }
    }

    private String resourcesJar() {

        String path = System.getProperty("java.class.path");
        if (path == null) {
            return null;
        }

        String[] paths = path.split(File.pathSeparator);

        List<String> jarPaths = new ArrayList<String>();
        for (String s : paths) {
            if (s.endsWith(".jar")) {
                jarPaths.add(s);
            }
        }

        /**
         * FIXME, this place seems exist problem
         */
        List<String> rtn = new ArrayList<String>();
        int size = jarPaths.size();
        for (int i = 0; i < size; i++) {
            if (JStormUtils.zipContainsDir(jarPaths.get(i), StormConfig.RESOURCES_SUBDIR)) {
                rtn.add(jarPaths.get(i));
            }
        }

        if (rtn.size() == 0)
            return null;

        return rtn.get(0);
    }

    /**
     * a port must be assigned one topology
     * 
     * @param stormClusterState
     * @param supervisorId
     * @throws Exception
     * @returns map: {port,LocalAssignment}
     */
    private Map<Integer, LocalAssignment> getLocalAssign(StormClusterState stormClusterState, String supervisorId, Map<String, Assignment> assignments)
            throws Exception {

        Map<Integer, LocalAssignment> portLA = new HashMap<Integer, LocalAssignment>();

        for (Entry<String, Assignment> assignEntry : assignments.entrySet()) {
            String topologyId = assignEntry.getKey();
            Assignment assignment = assignEntry.getValue();

            Map<Integer, LocalAssignment> portTasks = readMyTasks(stormClusterState, topologyId, supervisorId, assignment);
            if (portTasks == null) {
                continue;
            }

            // a port must be assigned one storm
            for (Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {

                Integer port = entry.getKey();

                LocalAssignment la = entry.getValue();

                if (!portLA.containsKey(port)) {
                    portLA.put(port, la);
                } else {
                    throw new RuntimeException("Should not have multiple topologys assigned to one port");
                }
            }
        }

        return portLA;
    }

    /**
     * get local node's tasks
     * 
     * @param stormClusterState
     * @param topologyId
     * @param supervisorId
     * @return Map: {port, LocalAssignment}
     * @throws Exception
     */
    private Map<Integer, LocalAssignment> readMyTasks(StormClusterState stormClusterState, String topologyId, String supervisorId, Assignment assignmentInfo)
            throws Exception {

        Map<Integer, LocalAssignment> portTasks = new HashMap<Integer, LocalAssignment>();

        Set<ResourceWorkerSlot> workers = assignmentInfo.getWorkers();
        if (workers == null) {
            LOG.error("No worker of assignment's " + assignmentInfo);
            return portTasks;
        }

        for (ResourceWorkerSlot worker : workers) {
            if (!supervisorId.equals(worker.getNodeId()))
                continue;
            portTasks.put(worker.getPort(), new LocalAssignment(topologyId, worker.getTasks(), Common.topologyIdToName(topologyId), worker.getMemSize(),
                    worker.getCpu(), worker.getJvm(), assignmentInfo.getTimeStamp()));
        }

        return portTasks;
    }

    /**
     * get mastercodedir for every topology
     * 
     * @throws Exception
     * @returns Map: <topologyId, master-code-dir> from zookeeper
     */
    public static Map<String, String> getTopologyCodeLocations(Map<String, Assignment> assignments, String supervisorId) throws Exception {

        Map<String, String> rtn = new HashMap<String, String>();
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            String topologyid = entry.getKey();
            Assignment assignmenInfo = entry.getValue();

            Set<ResourceWorkerSlot> workers = assignmenInfo.getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                String node = worker.getNodeId();
                if (supervisorId.equals(node)) {
                    rtn.put(topologyid, assignmenInfo.getMasterCodeDir());
                    break;
                }
            }

        }
        return rtn;
    }

    public void downloadTopology(Map<String, String> topologyCodes, List<String> downloadedTopologyIds, Set<String> updateTopologys,
            Map<String, Assignment> assignments, Set<String> downloadFailedTopologyIds) throws Exception {

        Set<String> downloadTopologys = new HashSet<String>();

        for (Entry<String, String> entry : topologyCodes.entrySet()) {

            String topologyId = entry.getKey();
            String masterCodeDir = entry.getValue();

            if (!downloadedTopologyIds.contains(topologyId) || updateTopologys.contains(topologyId)) {

                LOG.info("Downloading code for storm id " + topologyId + " from " + masterCodeDir);

                int retry = 0;
                while (retry < 3) {
                    try {
                        downloadStormCode(conf, topologyId, masterCodeDir);
                        // Update assignment timeStamp
                        StormConfig.write_supervisor_topology_timestamp(conf, topologyId, assignments.get(topologyId).getTimeStamp());
                        break;
                    } catch (IOException e) {
                        LOG.error(e + " downloadStormCode failed " + "topologyId:" + topologyId + "masterCodeDir:" + masterCodeDir);

                    } catch (TException e) {
                        LOG.error(e + " downloadStormCode failed " + "topologyId:" + topologyId + "masterCodeDir:" + masterCodeDir);
                    }
                    retry++;
                }
                if (retry < 3) {
                    LOG.info("Finished downloading code for storm id " + topologyId + " from " + masterCodeDir);
                    downloadTopologys.add(topologyId);
                } else {
                    LOG.error("Cann't  download code for storm id " + topologyId + " from " + masterCodeDir);
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
                    LOG.error("Cann't  clear directory about storm id " + topologyId + " on supervisor ");
                }
            }
        }

        updateTaskCleanupTimeout(downloadTopologys);
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

    private Set<String> getUpdateTopologys(Map<Integer, LocalAssignment> localAssignments, Map<Integer, LocalAssignment> zkAssignments,
            Map<String, Assignment> assignments) {
        Set<String> ret = new HashSet<String>();
        if (localAssignments != null && zkAssignments != null) {
            for (Entry<Integer, LocalAssignment> entry : localAssignments.entrySet()) {
                Integer port = entry.getKey();
                LocalAssignment localAssignment = entry.getValue();

                LocalAssignment zkAssignment = zkAssignments.get(port);

                if (localAssignment == null || zkAssignment == null)
                    continue;

                Assignment assignment = assignments.get(localAssignment.getTopologyId());
                if (localAssignment.getTopologyId().equals(zkAssignment.getTopologyId()) && assignment != null
                        && assignment.isTopologyChange(localAssignment.getTimeStamp()))
                    if (ret.add(localAssignment.getTopologyId())) {
                        LOG.info("Topology " + localAssignment.getTopologyId() + " has been updated. LocalTs=" + localAssignment.getTimeStamp() + ", ZkTs="
                                + zkAssignment.getTimeStamp());
                    }
            }
        }

        return ret;
    }

    private Set<String> getNeedReDownloadTopologys(Map<Integer, LocalAssignment> localAssignment) {
        Set<String> reDownloadTopologys = syncProcesses.getTopologyIdNeedDownload().getAndSet(null);
        if (reDownloadTopologys == null || reDownloadTopologys.size() == 0)
            return null;
        Set<String> needRemoveTopologys = new HashSet<String>();
        Map<Integer, String> portToStartWorkerId = syncProcesses.getPortToWorkerId();
        for (Entry<Integer, LocalAssignment> entry : localAssignment.entrySet()) {
            if (portToStartWorkerId.containsKey(entry.getKey()))
                needRemoveTopologys.add(entry.getValue().getTopologyId());
        }
        LOG.debug("worker is starting on these topology, so delay download topology binary: " + needRemoveTopologys);
        reDownloadTopologys.removeAll(needRemoveTopologys);
        if (reDownloadTopologys.size() > 0)
            LOG.info("Following topologys is going to re-download the jars, " + reDownloadTopologys);
        return reDownloadTopologys;
    }

    private void updateTaskCleanupTimeout(Set<String> topologys) {
        Map topologyConf = null;
        Map<String, Integer> taskCleanupTimeouts = new HashMap<String, Integer>();

        for (String topologyId : topologys) {
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


    private void getAllAssignments(Map<String, Integer> assignmentVersion, Map<String, Assignment> localZkAssignments,
            RunnableCallback callback) throws Exception {
        Map<String, Assignment> ret = new HashMap<String, Assignment>();
        Map<String, Integer> updateAssignmentVersion = new HashMap<String, Integer>();

        // get /assignments {topology_id}
        List<String> assignments = stormClusterState.assignments(callback);
        if (assignments == null) {
            assignmentVersion.clear();
            localZkAssignments.clear();
            LOG.debug("No assignment of ZK");
            return;
        }

        for (String topology_id : assignments) {

            Integer zkVersion = stormClusterState.assignment_version(topology_id, callback);
            LOG.debug(topology_id + "'s assigment version of zk is :" + zkVersion);
            Integer recordedVersion = assignmentVersion.get(topology_id);
            LOG.debug(topology_id + "'s assigment version of local is :" + recordedVersion);

            Assignment assignment = null;
            if (recordedVersion !=null && zkVersion !=null && recordedVersion.equals(zkVersion)) {
                assignment = localZkAssignments.get(topology_id);
            }
            //because the first version is 0
            if (assignment == null) {
                assignment = stormClusterState.assignment_info(topology_id, callback);
            }
            if (assignment == null) {
                LOG.error("Failed to get Assignment of " + topology_id + " from ZK");
                continue;
            }
            updateAssignmentVersion.put(topology_id, zkVersion);
            ret.put(topology_id, assignment);
        }
        assignmentVersion.clear();
        assignmentVersion.putAll(updateAssignmentVersion);
        localZkAssignments.clear();
        localZkAssignments.putAll(ret);
    }

}
