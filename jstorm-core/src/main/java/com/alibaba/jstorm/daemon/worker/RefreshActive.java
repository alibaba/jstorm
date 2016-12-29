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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.jstorm.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Periodically check whether a topology is active or not
 * and whether the metrics monitor is enabled or disabled from ZK
 *
 * @author yannian/Longda
 */
public class RefreshActive extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(RefreshActive.class);

    private WorkerData workerData;

    private AtomicBoolean monitorEnable;
    private Map<Object, Object> conf;
    private StormClusterState zkCluster;
    private String topologyId;
    private Integer frequency;

    // private Object lock = new Object();

    @SuppressWarnings("rawtypes")
    public RefreshActive(WorkerData workerData) {
        this.workerData = workerData;

        this.monitorEnable = workerData.getMonitorEnable();
        this.conf = workerData.getStormConf();
        this.zkCluster = workerData.getZkCluster();
        this.topologyId = workerData.getTopologyId();
        this.frequency = JStormUtils.parseInt(conf.get(Config.TASK_REFRESH_POLL_SECS), 10);
        LOG.info("init RefreshActive finished.");
        //call run() firstly to make task be active as soon as
        run();
    }

    @Override
    public void run() {
        try {
            StatusType newTopologyStatus;
            // /ZK-DIR/topology
            StormBase base = zkCluster.storm_base(topologyId, this);
            if (base == null) {
                // normally the topology has been removed
                LOG.warn("Failed to get StormBase from ZK of " + topologyId);
                newTopologyStatus = StatusType.killed;
            } else {
                newTopologyStatus = base.getStatus().getStatusType();
            }

            // Process the topology status change
            StatusType oldTopologyStatus = workerData.getTopologyStatus();

            List<TaskShutdownDameon> tasks = workerData.getShutdownTasks();
            if (tasks == null) {
                LOG.info("Tasks aren't ready or are beginning to shutdown");
                return;
            }

            // If initialization is on-going, check connection status first. 
            // If all connections were done, start to update topology status. Otherwise, just return.
            if (oldTopologyStatus == null) {
                if (!workerData.getWorkeInitConnectionStatus().get()) {
                    return;
                }
            }

            if (oldTopologyStatus == null || !newTopologyStatus.equals(oldTopologyStatus)) {
                LOG.info("Old TopologyStatus:" + oldTopologyStatus + ", new TopologyStatus:" + newTopologyStatus);
                if (newTopologyStatus.equals(StatusType.active)) {
                    for (TaskShutdownDameon task : tasks) {
                        if (task.getTask().getTaskStatus().isInit()) {
                            task.getTask().getTaskStatus().setStatus(TaskStatus.RUN);
                        } else {
                            task.active();
                        }
                    }
                } else if (oldTopologyStatus == null || !oldTopologyStatus.equals(StatusType.inactive)) {
                    for (TaskShutdownDameon task : tasks) {
                        if (task.getTask().getTaskStatus().isInit()) {
                            task.getTask().getTaskStatus().setStatus(TaskStatus.PAUSE);
                        } else {
                            task.deactive();
                        }
                    }
                }
                workerData.setTopologyStatus(newTopologyStatus);

                if (base != null) {
                    boolean newMonitorEnable = base.isEnableMonitor();
                    boolean oldMonitorEnable = monitorEnable.get();
                    if (newMonitorEnable != oldMonitorEnable) {
                        LOG.info("Change MonitorEnable from " + oldMonitorEnable + " to " + newMonitorEnable);
                        monitorEnable.set(newMonitorEnable);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get topology from ZK ", e);
        }

    }

    @Override
    public Object getResult() {
        return frequency;
    }
}
