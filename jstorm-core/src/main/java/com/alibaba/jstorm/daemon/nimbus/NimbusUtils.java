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
package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.NimbusStat;
import backtype.storm.generated.NimbusSummary;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TaskHeartbeat;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.blobstore.LocalFsBlobStore;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Sets;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class NimbusUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusUtils.class);

    /**
     * add custom KRYO serialization
     */
    private static Map mapifySerializations(List sers) {
        Map rtn = new HashMap();
        if (sers != null) {
            int size = sers.size();
            for (int i = 0; i < size; i++) {
                if (sers.get(i) instanceof Map) {
                    rtn.putAll((Map) sers.get(i));
                } else {
                    rtn.put(sers.get(i), null);
                }
            }

        }
        return rtn;
    }

    /**
     * Normalize stormConf
     *
     * @param conf      cluster conf
     * @param stormConf storm topology conf
     * @param topology  storm topology
     * @return normalized conf
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public static Map normalizeConf(Map conf, Map stormConf, StormTopology topology) throws Exception {

        List kryoRegisterList = new ArrayList();
        List kryoDecoratorList = new ArrayList();

        Map totalConf = new HashMap();
        totalConf.putAll(conf);
        totalConf.putAll(stormConf);

        Object totalRegister = totalConf.get(Config.TOPOLOGY_KRYO_REGISTER);
        if (totalRegister != null) {
            LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME) + ", TOPOLOGY_KRYO_REGISTER" +
                    totalRegister.getClass().getName());

            JStormUtils.mergeList(kryoRegisterList, totalRegister);
        }

        Object totalDecorator = totalConf.get(Config.TOPOLOGY_KRYO_DECORATORS);
        if (totalDecorator != null) {
            LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME) + ", TOPOLOGY_KRYO_DECORATOR" +
                    totalDecorator.getClass().getName());
            JStormUtils.mergeList(kryoDecoratorList, totalDecorator);
        }

        Set<String> cids = ThriftTopologyUtils.getComponentIds(topology);
        for (Iterator it = cids.iterator(); it.hasNext(); ) {
            String componentId = (String) it.next();

            ComponentCommon common = ThriftTopologyUtils.getComponentCommon(topology, componentId);
            String json = common.get_json_conf();
            if (json == null) {
                continue;
            }
            Map mtmp = (Map) JStormUtils.from_json(json);
            if (mtmp == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("Failed to deserialize " + componentId);
                sb.append(" json configuration: ");
                sb.append(json);
                LOG.info(sb.toString());
                throw new Exception(sb.toString());
            }

            Object componentKryoRegister = mtmp.get(Config.TOPOLOGY_KRYO_REGISTER);

            if (componentKryoRegister != null) {
                LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME) + ", componentId:" + componentId +
                        ", TOPOLOGY_KRYO_REGISTER" + componentKryoRegister.getClass().getName());

                JStormUtils.mergeList(kryoRegisterList, componentKryoRegister);
            }

            Object componentDecorator = mtmp.get(Config.TOPOLOGY_KRYO_DECORATORS);
            if (componentDecorator != null) {
                LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME) + ", componentId:" + componentId +
                        ", TOPOLOGY_KRYO_DECORATOR" + componentDecorator.getClass().getName());
                JStormUtils.mergeList(kryoDecoratorList, componentDecorator);
            }

            boolean isTransactionTopo = JStormUtils.parseBoolean(mtmp.get(ConfigExtension.TRANSACTION_TOPOLOGY), false);
            if (isTransactionTopo)
                stormConf.put(ConfigExtension.TRANSACTION_TOPOLOGY, true);
        }

        Map kryoRegisterMap = mapifySerializations(kryoRegisterList);
        List decoratorList = JStormUtils.distinctList(kryoDecoratorList);

        Integer ackerNum = JStormUtils.parseInt(totalConf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);

        Map rtn = new HashMap();
        //ensure to be cluster_mode
        rtn.put(Config.STORM_CLUSTER_MODE, conf.get(Config.STORM_CLUSTER_MODE));
        rtn.putAll(stormConf);
        rtn.put(Config.TOPOLOGY_KRYO_DECORATORS, decoratorList);
        rtn.put(Config.TOPOLOGY_KRYO_REGISTER, kryoRegisterMap);
        rtn.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackerNum);
        rtn.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, totalConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));

        return rtn;
    }

    public static Integer componentParalism(Map stormConf, ComponentCommon common) {
        Map mergeMap = new HashMap();
        mergeMap.putAll(stormConf);

        String jsonConfString = common.get_json_conf();
        if (jsonConfString != null) {
            Map componentMap = (Map) JStormUtils.from_json(jsonConfString);
            mergeMap.putAll(componentMap);
        }

        Integer taskNum = common.get_parallelism_hint();

        // don't get taskNum from component configuraiton
        // skip .setTaskNum
        // Integer taskNum = null;
        // Object taskNumObject = mergeMap.get(Config.TOPOLOGY_TASKS);
        // if (taskNumObject != null) {
        // taskNum = JStormUtils.parseInt(taskNumObject);
        // } else {
        // taskNum = common.get_parallelism_hint();
        // if (taskNum == null) {
        // taskNum = Integer.valueOf(1);
        // }
        // }

        Object maxTaskParalismObject = mergeMap.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
        if (maxTaskParalismObject == null) {
            return taskNum;
        } else {
            int maxTaskParalism = JStormUtils.parseInt(maxTaskParalismObject);

            return Math.min(maxTaskParalism, taskNum);
        }

    }

    /**
     * finalize component's task parallelism
     *
     * @param stormConf storm conf
     * @param topology  storm topology
     * @param fromConf  means if the parallelism is read from conf file instead of reading from topology code
     * @return normalized topology
     */
    public static StormTopology normalizeTopology(Map stormConf, StormTopology topology, boolean fromConf) {
        StormTopology ret = topology.deepCopy();

        Map<String, Object> rawComponents = ThriftTopologyUtils.getComponents(topology);

        Map<String, Object> components = ThriftTopologyUtils.getComponents(ret);

        if (!rawComponents.keySet().equals(components.keySet())) {
            String errMsg = "Failed to normalize topology binary!";
            LOG.info(errMsg + " raw components:" + rawComponents.keySet() + ", normalized " + components.keySet());

            throw new InvalidParameterException(errMsg);
        }

        for (Entry<String, Object> entry : components.entrySet()) {
            Object component = entry.getValue();
            String componentName = entry.getKey();

            ComponentCommon common = null;
            if (component instanceof Bolt) {
                common = ((Bolt) component).get_common();
                if (fromConf) {
                    Integer paraNum = ConfigExtension.getBoltParallelism(stormConf, componentName);
                    if (paraNum != null) {
                        LOG.info("Set " + componentName + " as " + paraNum);
                        common.set_parallelism_hint(paraNum);
                    }
                }
            }
            if (component instanceof SpoutSpec) {
                common = ((SpoutSpec) component).get_common();
                if (fromConf) {
                    Integer paraNum = ConfigExtension.getSpoutParallelism(stormConf, componentName);
                    if (paraNum != null) {
                        LOG.info("Set " + componentName + " as " + paraNum);
                        common.set_parallelism_hint(paraNum);
                    }
                }
            }
            if (component instanceof StateSpoutSpec) {
                common = ((StateSpoutSpec) component).get_common();
                if (fromConf) {
                    Integer paraNum = ConfigExtension.getSpoutParallelism(stormConf, componentName);
                    if (paraNum != null) {
                        LOG.info("Set " + componentName + " as " + paraNum);
                        common.set_parallelism_hint(paraNum);
                    }
                }
            }

            Map componentMap = new HashMap();

            String jsonConfString = common.get_json_conf();
            if (jsonConfString != null) {
                componentMap.putAll((Map) JStormUtils.from_json(jsonConfString));
            }

            Integer taskNum = componentParalism(stormConf, common);

            componentMap.put(Config.TOPOLOGY_TASKS, taskNum);
            // change the executor's task number
            common.set_parallelism_hint(taskNum);
            LOG.info("Set " + componentName + " parallelism " + taskNum);

            common.set_json_conf(JStormUtils.to_json(componentMap));
        }

        return ret;
    }

    private static void cleanupTopology(NimbusData data, String topologyId) throws Exception {
        StormClusterState stormClusterState = data.getStormClusterState();
        BlobStore blobStore = data.getBlobStore();
        stormClusterState.remove_storm(topologyId);
        if (blobStore instanceof LocalFsBlobStore) {
            List<String> blobKeys = BlobStoreUtils.getKeyListFromId(data, topologyId);
            for (String key : blobKeys) {
                stormClusterState.remove_blobstore_key(key);
                stormClusterState.remove_key_version(key);
            }
        }
    }

    /**
     * clean the topology which is in ZK but not in local dir
     */
    public static void cleanupCorruptTopologies(NimbusData data) throws Exception {
        BlobStore blobStore = data.getBlobStore();

        // we have only topology relative files , so we don't need filter
        Set<String> code_ids = Sets.newHashSet(BlobStoreUtils.code_ids(blobStore.listKeys()));
        // get topology in ZK /storms
        Set<String> active_ids = Sets.newHashSet(data.getStormClusterState().active_storms());
        //get topology in zk by blobs
        Set<String> blobsIdsOnZk = Sets.newHashSet(data.getStormClusterState().blobstore(null));
        Set<String> topologyIdsOnZkbyBlobs = BlobStoreUtils.code_ids(blobsIdsOnZk.iterator());

        Set<String> corrupt_ids = Sets.difference(active_ids, code_ids);
        Set<String> redundantIds = Sets.difference(topologyIdsOnZkbyBlobs, code_ids);
        Set<String> unionIds = Sets.union(corrupt_ids, redundantIds);
        // clean the topology which is in ZK but not in local dir
        for (String corrupt : unionIds) {
            LOG.info("Corrupt topology {} has state on zookeeper but doesn't have a local dir on nimbus. Cleaning up...", corrupt);
            try {
                cleanupTopology(data, corrupt);
            } catch (Exception e) {
                LOG.warn("Failed to cleanup topology {}, {}", corrupt, e);
            }
        }

        LOG.info("Successfully cleaned up all old topologies");
    }

    public static boolean isTaskDead(NimbusData data, String topologyId, Integer taskId) {
        String idStr = " topology:" + topologyId + ",task id:" + taskId;

        TopologyTaskHbInfo topoTasksHbInfo = data.getTasksHeartbeat().get(topologyId);
        Map<Integer, TaskHeartbeat> taskHbMap = null;
        Integer taskReportTime = null;

        if (topoTasksHbInfo != null) {
            taskHbMap = topoTasksHbInfo.get_taskHbs();
            if (taskHbMap != null) {
                TaskHeartbeat tHb = taskHbMap.get(taskId);
                taskReportTime = ((tHb != null) ? tHb.get_time() : null);
            }
        }

        Map<Integer, TkHbCacheTime> taskHBs = data.getTaskHeartbeatsCache(topologyId, true);

        TkHbCacheTime taskHB = taskHBs.get(taskId);
        if (taskHB == null) {
            LOG.debug("No task heartbeat cache " + idStr);
            if (topoTasksHbInfo == null || taskHbMap == null) {
                LOG.info("No task heartbeat was reported for " + idStr);
                return true;
            }

            taskHB = new TkHbCacheTime();
            taskHB.update(taskHbMap.get(taskId));

            taskHBs.put(taskId, taskHB);

            return false;
        }

        if (taskReportTime == null || taskReportTime < taskHB.getTaskAssignedTime()) {
            LOG.debug("No task heartbeat was reported for " + idStr);
            // Task hasn't finish init
            int nowSecs = TimeUtils.current_time_secs();
            int assignSecs = taskHB.getTaskAssignedTime();

            // default to 4 min
            int waitInitTimeout = JStormUtils.parseInt(data.getConf().get(Config.NIMBUS_TASK_LAUNCH_SECS));
            if (nowSecs - assignSecs > waitInitTimeout) {
                LOG.info(idStr + " failed to init ");
                return true;
            } else {
                return false;
            }
        }

        // the left is zkReportTime isn't null
        // task has finished initialization
        int nimbusTime = taskHB.getNimbusTime();
        int reportTime = taskHB.getTaskReportedTime();

        int nowSecs = TimeUtils.current_time_secs();
        if (nimbusTime == 0) {
            // taskHB no entry, first time
            // update taskHBtaskReportTime
            taskHB.setNimbusTime(nowSecs);
            taskHB.setTaskReportedTime(taskReportTime);

            LOG.info("Update task heartbeat to nimbus cache " + idStr);
            return false;
        }

        if (reportTime != taskReportTime) {
            // zk has been updated the report time
            taskHB.setNimbusTime(nowSecs);
            taskHB.setTaskReportedTime(taskReportTime);

            LOG.debug(idStr + ",nimbusTime " + nowSecs + ",zkReport:" + taskReportTime + ",report:" + reportTime);
            return false;
        }

        // the following is (zkReportTime == reportTime)
        Integer taskHBTimeout = data.getTopologyTaskTimeout().get(topologyId);
        if (taskHBTimeout == null)
            // default to 2 min
            taskHBTimeout = JStormUtils.parseInt(data.getConf().get(Config.NIMBUS_TASK_TIMEOUT_SECS));
        if (taskId == topoTasksHbInfo.get_topologyMasterId())
            taskHBTimeout = (taskHBTimeout / 2);
        if (nowSecs - nimbusTime > taskHBTimeout) {
            // task is dead
            long ts = ((long) nimbusTime) * 1000;
            Date lastTaskHBDate = new Date(ts);
            LOG.debug(idStr + " last task time is " + nimbusTime + ":" + lastTaskHBDate + ",current " +
                    nowSecs + ":" + new Date(((long) nowSecs) * 1000));
            return true;
        }

        return false;
    }

    public static void updateTaskHbStartTime(NimbusData data, Assignment assignment, String topologyId) {
        Map<Integer, TkHbCacheTime> taskHBs = data.getTaskHeartbeatsCache(topologyId, true);

        Map<Integer, Integer> taskStartTimes = assignment.getTaskStartTimeSecs();
        for (Entry<Integer, Integer> entry : taskStartTimes.entrySet()) {
            Integer taskId = entry.getKey();
            Integer taskStartTime = entry.getValue();

            TkHbCacheTime taskHB = taskHBs.get(taskId);
            if (taskHB == null) {
                taskHB = new TkHbCacheTime();
                taskHBs.put(taskId, taskHB);
            }

            taskHB.setTaskAssignedTime(taskStartTime);
        }
    }

    public static <T> void transitionName(NimbusData data, String topologyName, boolean errorOnNoTransition,
                                          StatusType transition_status, T... args)
            throws Exception {
        StormClusterState stormClusterState = data.getStormClusterState();
        String topologyId = Cluster.get_topology_id(stormClusterState, topologyName);
        if (topologyId == null) {
            throw new NotAliveException(topologyName);
        }
        transition(data, topologyId, errorOnNoTransition, transition_status, args);
    }

    public static <T> void transition(NimbusData data, String topologyid, boolean errorOnNoTransition,
                                      StatusType transition_status, T... args) {
        try {
            data.getStatusTransition().transition(topologyid, errorOnNoTransition, transition_status, args);
        } catch (Exception e) {
            LOG.error("Failed to do status transition,", e);
        }
    }

    public static int getTopologyTaskNum(Assignment assignment) {
        int numTasks = 0;

        for (ResourceWorkerSlot worker : assignment.getWorkers()) {
            numTasks += worker.getTasks().size();
        }
        return numTasks;
    }

    public static List<TopologySummary> getTopologySummary(StormClusterState stormClusterState,
                                                           Map<String, Assignment> assignments) throws Exception {
        List<TopologySummary> topologySummaries = new ArrayList<>();

        // get all active topology's StormBase
        Map<String, StormBase> bases = Cluster.get_all_StormBase(stormClusterState);
        for (Entry<String, StormBase> entry : bases.entrySet()) {

            String topologyId = entry.getKey();
            StormBase base = entry.getValue();

            Assignment assignment = stormClusterState.assignment_info(topologyId, null);
            if (assignment == null) {
                LOG.error("Failed to get assignment of topology: " + topologyId);
                continue;
            }
            assignments.put(topologyId, assignment);

            int num_workers = assignment.getWorkers().size();
            int num_tasks = getTopologyTaskNum(assignment);

            String errorString;
            if (Cluster.is_topology_exist_error(stormClusterState, topologyId)) {
                errorString = "Y";
            } else {
                errorString = "";
            }

            TopologySummary topology = new TopologySummary();
            topology.set_id(topologyId);
            topology.set_name(base.getStormName());
            topology.set_status(base.getStatusString());
            topology.set_uptimeSecs(TimeUtils.time_delta(base.getLanchTimeSecs()));
            topology.set_numWorkers(num_workers);
            topology.set_numTasks(num_tasks);
            topology.set_errorInfo(errorString);

            topologySummaries.add(topology);

        }

        return topologySummaries;
    }

    public static SupervisorSummary mkSupervisorSummary(SupervisorInfo supervisorInfo, String supervisorId,
                                                        Map<String, Integer> supervisorToUsedSlotNum) {
        Integer usedNum = supervisorToUsedSlotNum.get(supervisorId);

        SupervisorSummary summary = new SupervisorSummary(supervisorInfo.getHostName(), supervisorId,
                supervisorInfo.getUptimeSecs(), supervisorInfo.getWorkerPorts().size(), usedNum == null ? 0 : usedNum);
        summary.set_version(supervisorInfo.getVersion());
        summary.set_buildTs(supervisorInfo.getBuildTs());
        summary.set_port(supervisorInfo.getPort() != null ? supervisorInfo.getPort() : 0);
        summary.set_errorMessage(supervisorInfo.getErrorMessage());

        return summary;
    }

    public static List<SupervisorSummary> mkSupervisorSummaries(Map<String, SupervisorInfo> supervisorInfos,
                                                                Map<String, Assignment> assignments) {

        Map<String, Integer> supervisorToLeftSlotNum = new HashMap<>();
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            Set<ResourceWorkerSlot> workers = entry.getValue().getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                String supervisorId = worker.getNodeId();
                SupervisorInfo supervisorInfo = supervisorInfos.get(supervisorId);
                if (supervisorInfo == null) {
                    continue;
                }
                Integer slots = supervisorToLeftSlotNum.get(supervisorId);
                if (slots == null) {
                    slots = 0;
                    supervisorToLeftSlotNum.put(supervisorId, slots);
                }
                supervisorToLeftSlotNum.put(supervisorId, ++slots);
            }
        }

        List<SupervisorSummary> ret = new ArrayList<>();
        for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorInfo supervisorInfo = entry.getValue();
            SupervisorSummary summary = mkSupervisorSummary(supervisorInfo, supervisorId, supervisorToLeftSlotNum);
            ret.add(summary);
        }

        Collections.sort(ret, new Comparator<SupervisorSummary>() {

            @Override
            public int compare(SupervisorSummary o1, SupervisorSummary o2) {
                return o1.get_host().compareTo(o2.get_host());
            }

        });
        return ret;
    }

    public static NimbusSummary getNimbusSummary(StormClusterState stormClusterState,
                                                 List<SupervisorSummary> supervisorSummaries, NimbusData data)
            throws Exception {
        NimbusSummary ret = new NimbusSummary();

        String master = stormClusterState.get_leader_host();
        NimbusStat nimbusMaster = new NimbusStat();
        nimbusMaster.set_host(master);
        nimbusMaster.set_uptimeSecs(String.valueOf(data.uptime()));
        ret.set_nimbusMaster(nimbusMaster);

        List<NimbusStat> nimbusSlaveList = new ArrayList<>();
        ret.set_nimbusSlaves(nimbusSlaveList);
        Map<String, String> nimbusSlaveMap = Cluster.get_all_nimbus_slave(stormClusterState);
        if (nimbusSlaveMap != null) {
            for (Entry<String, String> entry : nimbusSlaveMap.entrySet()) {
                NimbusStat slave = new NimbusStat();
                slave.set_host(entry.getKey());
                slave.set_uptimeSecs(entry.getValue());

                nimbusSlaveList.add(slave);
            }
        }

        int totalPort = 0;
        int usedPort = 0;

        for (SupervisorSummary supervisor : supervisorSummaries) {
            totalPort += supervisor.get_numWorkers();
            usedPort += supervisor.get_numUsedWorkers();
        }

        ret.set_supervisorNum(supervisorSummaries.size());
        ret.set_totalPortNum(totalPort);
        ret.set_usedPortNum(usedPort);
        ret.set_freePortNum(totalPort - usedPort);
        ret.set_version(Utils.getVersion());

        return ret;
    }

    public static void updateTopologyTaskTimeout(NimbusData data, String topologyId) {
        Map topologyConf = null;
        try {
            topologyConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
        } catch (Exception e) {
            LOG.warn("Failed to read configuration of {}, {}", topologyId, e.getMessage());
        }

        Integer timeout = null;
        if (topologyConf != null) {
            timeout = JStormUtils.parseInt(topologyConf.get(Config.NIMBUS_TASK_TIMEOUT_SECS));
        }
        if (timeout == null) {
            timeout = JStormUtils.parseInt(data.getConf().get(Config.NIMBUS_TASK_TIMEOUT_SECS));
        }
        LOG.info("Setting taskTimeout:" + timeout + " for " + topologyId);
        data.getTopologyTaskTimeout().put(topologyId, timeout);
    }

    public static void removeTopologyTaskTimeout(NimbusData data, String topologyId) {
        data.getTopologyTaskTimeout().remove(topologyId);
    }

    public static void updateTopologyTaskHb(NimbusData data, String topologyId) {
        StormClusterState clusterState = data.getStormClusterState();
        TopologyTaskHbInfo topologyTaskHb = null;

        try {
            topologyTaskHb = clusterState.topology_heartbeat(topologyId);
        } catch (Exception e) {
            LOG.error("updateTopologyTaskHb: Failed to get topology task heartbeat info", e);
        }

        if (topologyTaskHb != null) {
            data.getTasksHeartbeat().put(topologyId, topologyTaskHb);
        }
    }

    public static void removeTopologyTaskHb(NimbusData data, String topologyId, int taskId) {
        TopologyTaskHbInfo topologyTaskHbs = data.getTasksHeartbeat().get(topologyId);

        if (topologyTaskHbs != null) {
            Map<Integer, TaskHeartbeat> taskHbs = topologyTaskHbs.get_taskHbs();
            if (taskHbs != null) {
                taskHbs.remove(taskId);
            }
        }
    }

    public static int getTopologyMasterId(Map<Integer, TaskInfo> tasksInfo) {
        int ret = 0;
        for (Entry<Integer, TaskInfo> entry : tasksInfo.entrySet()) {
            if (entry.getValue().getComponentId().equalsIgnoreCase(Common.TOPOLOGY_MASTER_COMPONENT_ID)) {
                ret = entry.getKey();
                break;
            }
        }

        return ret;
    }
}
