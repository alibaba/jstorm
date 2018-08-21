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
package com.alibaba.jstorm.callback.impl;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.*;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * Do real rebalance action.
 *
 * After nimbus receives a rebalance command, it will do the following:
 * 1. set topology status to rebalancing
 * 2. delay 2 * timeout seconds
 * 3. execute this callback
 *
 * @author Xin.Li/Longda
 */
public class DoRebalanceTransitionCallback extends BaseCallback {
    private static final Logger LOG = LoggerFactory.getLogger(DoRebalanceTransitionCallback.class);

    private NimbusData data;
    private String topologyId;
    private StormStatus oldStatus;
    private Set<Integer> newTasks;

    public DoRebalanceTransitionCallback(NimbusData data, String topologyId, StormStatus status) {
        this.data = data;
        this.topologyId = topologyId;
        this.oldStatus = status;
        this.newTasks = new HashSet<>();
    }

    @Override
    public <T> Object execute(T... args) {
        boolean isSetTaskInfo = false;
        try {
            Boolean reassign = (Boolean) args[1];
            // args[0]: delay, args[1]: reassign_flag, args[2]: conf
            Map<Object, Object> conf = (Map<Object, Object>) args[2];
            if (conf != null) {
                boolean isConfUpdate = false;

                // Update topology code
                Map topoConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
                StormTopology rawOldTopology = StormConfig.read_nimbus_topology_code(topologyId, data.getBlobStore());
                StormTopology rawNewTopology = NimbusUtils.normalizeTopology(conf, rawOldTopology, true);
                StormTopology sysOldTopology = rawOldTopology.deepCopy();
                StormTopology sysNewTopology = rawNewTopology.deepCopy();
                if (conf.get(Config.TOPOLOGY_ACKER_EXECUTORS) != null) {
                    Common.add_acker(topoConf, sysOldTopology);
                    Common.add_acker(conf, sysNewTopology);
                    int ackerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
                    int oldAckerNum = JStormUtils.parseInt(topoConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
                    LOG.info("Update acker from oldAckerNum=" + oldAckerNum + " to ackerNum=" + ackerNum);
                    topoConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackerNum);
                    isConfUpdate = true;
                }

                // If scale-out, setup task info for new added tasks
                setTaskInfo(sysOldTopology, sysNewTopology);
                isSetTaskInfo = true;

                // If everything is OK, write topology code into disk
                StormConfig.write_nimbus_topology_code(topologyId, Utils.serialize(rawNewTopology), data);

                // Update topology conf if worker num has been updated
                Set<Object> keys = conf.keySet();
                Integer workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS));
                if (workerNum != null) {
                    Integer oldWorkerNum = JStormUtils.parseInt(topoConf.get(Config.TOPOLOGY_WORKERS));
                    topoConf.put(Config.TOPOLOGY_WORKERS, workerNum);
                    isConfUpdate = true;

                    LOG.info("Update worker num from " + oldWorkerNum + " to " + workerNum);
                }

                if (keys.contains(Config.ISOLATION_SCHEDULER_MACHINES)) {
                    topoConf.put(Config.ISOLATION_SCHEDULER_MACHINES, conf.get(Config.ISOLATION_SCHEDULER_MACHINES));
                }

                if (isConfUpdate) {
                    StormConfig.write_nimbus_topology_conf(topologyId, topoConf, data);
                }
            }

            TopologyAssignEvent event = new TopologyAssignEvent();

            event.setTopologyId(topologyId);
            event.setScratch(true);
            event.setOldStatus(oldStatus);
            event.setReassign(reassign);
            if (conf != null)
                event.setScaleTopology(true);
            TopologyAssign.push(event);
            event.waitFinish();
        } catch (Exception e) {
            LOG.error("do-rebalance error!", e);
            // Rollback the changes on ZK
            if (isSetTaskInfo) {
                try {
                    StormClusterState clusterState = data.getStormClusterState();
                    clusterState.remove_task(topologyId, newTasks);
                } catch (Exception e1) {
                    LOG.error("Failed to rollback the changes on ZK for task-" + newTasks, e);
                }
            }
        }

        DelayStatusTransitionCallback delayCallback =
                new DelayStatusTransitionCallback(data, topologyId, oldStatus, StatusType.rebalancing, StatusType.done_rebalance);
        return delayCallback.execute();
    }

    private void setTaskInfo(StormTopology oldTopology, StormTopology newTopology) throws Exception {
        StormClusterState clusterState = data.getStormClusterState();
        // Retrieve the max task ID
        TreeSet<Integer> taskIds = new TreeSet<>(clusterState.task_ids(topologyId));
        int cnt = taskIds.descendingIterator().next();

        cnt = setBoltInfo(oldTopology, newTopology, cnt, clusterState);
        cnt = setSpoutInfo(oldTopology, newTopology, cnt, clusterState);
    }

    private int setBoltInfo(StormTopology oldTopology, StormTopology newTopology,
                            int cnt, StormClusterState clusterState) throws Exception {
        Map<String, Bolt> oldBolts = oldTopology.get_bolts();
        Map<String, Bolt> bolts = newTopology.get_bolts();
        for (Entry<String, Bolt> entry : oldBolts.entrySet()) {
            String boltName = entry.getKey();
            Bolt oldBolt = entry.getValue();
            Bolt bolt = bolts.get(boltName);
            if (oldBolt.get_common().get_parallelism_hint() > bolt.get_common().get_parallelism_hint()) {
                int removedTaskNum = oldBolt.get_common().get_parallelism_hint() - bolt.get_common().get_parallelism_hint();
                TreeSet<Integer> taskIds = new TreeSet<>(clusterState.task_ids_by_componentId(topologyId, boltName));
                Iterator<Integer> descendIterator = taskIds.descendingIterator();
                while (--removedTaskNum >= 0) {
                    int taskId = descendIterator.next();
                    removeTask(topologyId, taskId, clusterState);
                    LOG.info("Remove bolt task, taskId=" + taskId + " for " + boltName);
                }
            } else if (oldBolt.get_common().get_parallelism_hint() == bolt.get_common().get_parallelism_hint()) {
                continue;
            } else {
                int delta = bolt.get_common().get_parallelism_hint() - oldBolt.get_common().get_parallelism_hint();
                Map<Integer, TaskInfo> taskInfoMap = new HashMap<>();

                for (int i = 1; i <= delta; i++) {
                    cnt++;
                    TaskInfo taskInfo = new TaskInfo(entry.getKey(), "bolt");
                    taskInfoMap.put(cnt, taskInfo);
                    newTasks.add(cnt);
                    LOG.info("Setup new bolt task, taskId=" + cnt + " for " + boltName);
                }
                clusterState.add_task(topologyId, taskInfoMap);
            }
        }

        return cnt;
    }

    private int setSpoutInfo(StormTopology oldTopology, StormTopology newTopology,
                             int cnt, StormClusterState clusterState) throws Exception {
        Map<String, SpoutSpec> oldSpouts = oldTopology.get_spouts();
        Map<String, SpoutSpec> spouts = newTopology.get_spouts();
        for (Entry<String, SpoutSpec> entry : oldSpouts.entrySet()) {
            String spoutName = entry.getKey();
            SpoutSpec oldSpout = entry.getValue();
            SpoutSpec spout = spouts.get(spoutName);
            if (oldSpout.get_common().get_parallelism_hint() > spout.get_common().get_parallelism_hint()) {
                int removedTaskNum = oldSpout.get_common().get_parallelism_hint() - spout.get_common().get_parallelism_hint();
                TreeSet<Integer> taskIds = new TreeSet<>(clusterState.task_ids_by_componentId(topologyId, spoutName));
                Iterator<Integer> descendIterator = taskIds.descendingIterator();
                while (--removedTaskNum >= 0) {
                    int taskId = descendIterator.next();
                    removeTask(topologyId, taskId, clusterState);
                    LOG.info("Remove spout task, taskId=" + taskId + " for " + spoutName);
                }
            } else if (oldSpout.get_common().get_parallelism_hint() == spout.get_common().get_parallelism_hint()) {
                continue;
            } else {
                int delta = spout.get_common().get_parallelism_hint() - oldSpout.get_common().get_parallelism_hint();
                Map<Integer, TaskInfo> taskInfoMap = new HashMap<>();

                for (int i = 1; i <= delta; i++) {
                    cnt++;
                    TaskInfo taskInfo = new TaskInfo(entry.getKey(), "spout");
                    taskInfoMap.put(cnt, taskInfo);
                    newTasks.add(cnt);
                    LOG.info("Setup new spout task, taskId=" + cnt + " for " + spoutName);
                }
                clusterState.add_task(topologyId, taskInfoMap);
            }
        }

        return cnt;
    }

    private void removeTask(String topologyId, int taskId, StormClusterState clusterState) throws Exception {
        Set<Integer> taskIds = new HashSet<>(taskId);
        clusterState.remove_task(this.topologyId, taskIds);
        Map<Integer, TkHbCacheTime> tkHbs = data.getTaskHeartbeatsCache(this.topologyId, false);
        if (tkHbs != null) {
            tkHbs.remove(taskId);
        }
    }
}
