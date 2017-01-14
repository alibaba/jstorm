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
package com.alibaba.jstorm.task.master.heartbeat;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.jstorm.task.error.ErrorConstants;
import backtype.storm.generated.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.execute.BoltCollector;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.UpdateConfigEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMasterContext;
import com.alibaba.jstorm.utils.TimeUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClientWrapper;
import backtype.storm.utils.Utils;

/**
 * Update the task heartbeat information of topology to Nimbus
 *
 * @author Basti Liu
 */
public class TaskHeartbeatUpdater implements TMHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskHeartbeatUpdater.class);

    private int MAX_NUM_TASK_HB_SEND;

    private String topologyId;
    private int taskId;

    private Map stormConf;
    private NimbusClientWrapper client;

    private AtomicReference<ConcurrentHashMap<Integer, TaskHeartbeat>> taskHbMap;
    private TopologyTaskHbInfo taskHbs;

    private StormClusterState zkCluster;

    private TopologyContext context;

    //TODO: topologyTasks will be changed when update topology!
    private ConcurrentHashMap<Integer, TaskStatus> boltsExecutorStatusMap; //exception of topology master task
    private ConcurrentHashMap<Integer, TaskStatus> spoutsExecutorStatusMap;

    private OutputCollector collector;
    private final Object _lock = new Object();

    public TaskHeartbeatUpdater() {
    }

    @Override
    public void process(Object event) throws Exception {
        synchronized (_lock) {
            if (event instanceof UpdateConfigEvent) {
                update(((UpdateConfigEvent) event).getConf());
                return;
            }

            Tuple input = (Tuple) event;
            int sourceTask = input.getSourceTask();
            int uptime = (Integer) input.getValue(0);
            TaskStatus executorStatus = new TaskStatus();
            if (input.getValues().size() < 2) {
                // for compatibility
                executorStatus.setStatus(TaskStatus.RUN);
            } else {
                executorStatus.setStatus((byte) input.getValue(1));
            }
            boolean isSendSpoutTaskFinishStream = false;

            if (spoutsExecutorStatusMap.containsKey(sourceTask)) {
                spoutsExecutorStatusMap.put(sourceTask, executorStatus);
            } else if (boltsExecutorStatusMap.containsKey(sourceTask)) {
                boltsExecutorStatusMap.put(sourceTask, executorStatus);
            } else if (sourceTask != taskId) {
                LOG.warn("received invalid task heartbeat {}", input);
            }
            if (executorStatus.getStatus() == TaskStatus.INIT && spoutsExecutorStatusMap.get(sourceTask) != null) {
                boolean existInitStatusBolt = false;
                for (TaskStatus status : boltsExecutorStatusMap.values()) {
                    if (status.getStatus() == TaskStatus.INIT || status.getStatus() == TaskStatus.SHUTDOWN) {
                        existInitStatusBolt = true;
                        break;
                    }
                }
                if (!existInitStatusBolt)
                    isSendSpoutTaskFinishStream = true;
            }

            if (client == null) {
                client = new NimbusClientWrapper();
                client.init(stormConf);
            }

            // Update the heartbeat for source task, but don't update it if task is initial status
            if (executorStatus.getStatus() != TaskStatus.INIT && uptime > 0) {
                TaskHeartbeat taskHb = taskHbMap.get().get(sourceTask);
                if (taskHb == null) {
                    taskHb = new TaskHeartbeat(TimeUtils.current_time_secs(), uptime);
                    TaskHeartbeat tmpTaskHb = taskHbMap.get().putIfAbsent(sourceTask, taskHb);
                    if (tmpTaskHb != null) {
                        taskHb = tmpTaskHb;
                    }
                }
                taskHb.set_time(TimeUtils.current_time_secs());
                taskHb.set_uptime(uptime);
            } else if (isSendSpoutTaskFinishStream) {
                TopoMasterCtrlEvent finishInitEvent = new TopoMasterCtrlEvent(TopoMasterCtrlEvent.EventType.topologyFinishInit);
                ((BoltCollector) (collector.getDelegate())).emitDirectCtrl(sourceTask, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null,
                        new Values(finishInitEvent));
                LOG.info("all bolts' task finish init operation, so tm will notify the spout task-{}", sourceTask);
            }

            // Send heartbeat info of all tasks to nimbus
            if (sourceTask == taskId) {
                uploadHB();
            }
        }
    }

    public void uploadHB() throws Exception {
        Map<Integer, TaskHeartbeat> tmpTaskHbMap = new ConcurrentHashMap<>();

        int sendCount = 0;
        ConcurrentHashMap<Integer, TaskHeartbeat> oldTaskHbMap =
                taskHbMap.getAndSet(new ConcurrentHashMap<Integer, TaskHeartbeat>());
        for (Entry<Integer, TaskHeartbeat> entry : oldTaskHbMap.entrySet()) {
            tmpTaskHbMap.put(entry.getKey(), entry.getValue());
            sendCount++;

            if (sendCount >= MAX_NUM_TASK_HB_SEND) {
                uploadTaskHeatbeat(tmpTaskHbMap);
                tmpTaskHbMap = new ConcurrentHashMap<>();
                sendCount = 0;
            }
        }
        if (tmpTaskHbMap.size() > 0) {
            uploadTaskHeatbeat(tmpTaskHbMap);
        }
    }

    public void initTaskHb() {
        this.taskHbs = new TopologyTaskHbInfo(this.topologyId, this.taskId);
        ConcurrentHashMap<Integer, TaskHeartbeat> tmpTaskHbMap = new ConcurrentHashMap<>();
        try {

            TopologyTaskHbInfo taskHbInfo = zkCluster.topology_heartbeat(topologyId);
            if (taskHbInfo != null) {
                LOG.info("Found task heartbeat info left in zk for " + topologyId + ": " + taskHbInfo.toString());

                if (taskHbInfo.get_taskHbs() != null) {
                    tmpTaskHbMap.putAll(taskHbInfo.get_taskHbs());
                }
            }

        } catch (Exception e) {
            LOG.warn("Failed to get topology heartbeat from zk", e);
        }

        this.taskHbMap.set(tmpTaskHbMap);
        taskHbs.set_taskHbs(tmpTaskHbMap);
    }

    public void init(TopologyMasterContext tmContext) {
        this.topologyId = tmContext.getTopologyId();
        this.taskId = tmContext.getTaskId();
        this.context = tmContext.getContext();
        this.collector = tmContext.getCollector();

        this.stormConf = tmContext.getConf();


        this.zkCluster = tmContext.getZkCluster();
        this.taskHbMap = new AtomicReference<>();

        initExecutorsStatus();

        initTaskHb();
        this.MAX_NUM_TASK_HB_SEND = ConfigExtension.getTopologyTaskHbSendNumber(stormConf);
    }

    private void initExecutorsStatus() {
        this.boltsExecutorStatusMap = new ConcurrentHashMap<>();
        this.spoutsExecutorStatusMap = new ConcurrentHashMap<>();
        try {
            StormTopology sysTopology = Common.system_topology(stormConf, context.getRawTopology());
            Map<String, Bolt> bolts = sysTopology.get_bolts();
            if (bolts != null) {
                List<Integer> boltTasks = this.context.getComponentsTasks(bolts.keySet());
                for (Integer task : boltTasks) {
                    boltsExecutorStatusMap.put(task, new TaskStatus());
                }
                //exception of topolgy master task
                boltsExecutorStatusMap.remove(taskId);
            }
            Map<String, SpoutSpec> spouts = sysTopology.get_spouts();
            if (bolts != null) {
                List<Integer> spoutTasks = this.context.getComponentsTasks(spouts.keySet());
                for (Integer task : spoutTasks) {
                    spoutsExecutorStatusMap.put(task, new TaskStatus());
                }
            }
        } catch (InvalidTopologyException e) {
            LOG.error("Failed to build system topology", e);
            throw new RuntimeException(e);
        }
    }

    private void uploadTaskHeatbeat(Map<Integer, TaskHeartbeat> tmpTaskHbMap) throws Exception {
        try {
            if (tmpTaskHbMap == null || tmpTaskHbMap.size() == 0) {
                return;
            }

            TopologyTaskHbInfo topologyTaskHbInfo = new TopologyTaskHbInfo(topologyId, taskId);
            topologyTaskHbInfo.set_taskHbs(tmpTaskHbMap);

            client.getClient().updateTaskHeartbeat(topologyTaskHbInfo);

            String info = "";
            for (Entry<Integer, TaskHeartbeat> entry : topologyTaskHbInfo.get_taskHbs().entrySet()) {
                info += " " + entry.getKey() + "-" + entry.getValue().get_time();
            }
            LOG.info("Update task heartbeat:" + info);
        } catch (Exception e) {
            String errorInfo = "Failed to update task heartbeat info";
            LOG.error("Failed to update task heartbeat info ", e);
            if (client != null) {
                client.cleanup();
                client = null;
            }
            zkCluster.report_task_error(context.getTopologyId(), context.getThisTaskId(), errorInfo,
                    ErrorConstants.WARN, ErrorConstants.CODE_USER);
        }
    }

    @Override
    public void cleanup() {
        try {
            StormBase stormBase = zkCluster.storm_base(topologyId, null);
            boolean isKilledStatus = stormBase != null && stormBase.getStatus().getStatusType().equals(StatusType.killed);
            if (stormBase == null || isKilledStatus) {
                // wait for pendings to exit
                final long timeoutMilliSeconds = 1000 * ConfigExtension.getTaskCleanupTimeoutSec(stormConf);
                final long start = System.currentTimeMillis();
                while (true) {
                    try {
                        long delta = System.currentTimeMillis() - start;
                        if (delta > timeoutMilliSeconds) {
                            LOG.warn("Timeout when waiting others' tasks shutdown");
                            break;
                        }
                        if (checkAllTasksShutdown()) {
                            break;
                        }
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("notify nimbus that all tasks's status is: {} {} ", spoutsExecutorStatusMap.toString(),
                        boltsExecutorStatusMap.toString());
                if (client == null) {
                    client = new NimbusClientWrapper();
                    client.init(stormConf);
                }
                client.getClient().notifyThisTopologyTasksIsDead(topologyId);

            }
        } catch (Exception e) {
            if (client != null) {
                client.cleanup();
                client = null;
            }
            LOG.warn("Failed to get topology stormbase from zk", e);
        }
    }

    private boolean checkAllTasksShutdown() {
        boolean ret = true;
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.addAll(spoutsExecutorStatusMap.values());
        taskStatuses.addAll(boltsExecutorStatusMap.values());
        for (TaskStatus status : taskStatuses) {
            if (status.getStatus() == TaskStatus.RUN) {
                ret = false;
                break;
            }
        }
        return ret;
    }

    public void update(Map conf) {
        LOG.info("Topology master received new conf:" + conf);
        synchronized (_lock) {
            initExecutorsStatus();
        }
    }
}
