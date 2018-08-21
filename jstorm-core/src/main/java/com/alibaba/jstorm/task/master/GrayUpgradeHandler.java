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
package com.alibaba.jstorm.task.master;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.upgrade.GrayUpgradeConfig;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wange
 * @since 2.3.1
 */
public class GrayUpgradeHandler implements TMHandler, Runnable {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private StormClusterState stormClusterState;

    private String topologyId;
    private TopologyMasterContext tmContext;
    private Map<String, Set<Integer>> hostPortToTasks;
    private Map<Integer, String> taskToHostPort;

    private Set<String> totalWorkers;

    @Override
    public void init(TopologyMasterContext tmContext) {
        this.tmContext = tmContext;

        this.stormClusterState = tmContext.getZkCluster();
        this.topologyId = tmContext.getTopologyId();

        this.hostPortToTasks = new HashMap<>();
        this.taskToHostPort = new HashMap<>();
        for (ResourceWorkerSlot workerSlot : tmContext.getWorkerSet().get()) {
            Set<Integer> tasks = workerSlot.getTasks();
            String hostPort = workerSlot.getHostPort();
            hostPortToTasks.put(hostPort, Sets.newHashSet(tasks));

            for (Integer task : tasks) {
                this.taskToHostPort.put(task, hostPort);
            }
        }

        this.totalWorkers = new HashSet<>();
    }

    @Override
    public void process(Object event) throws Exception {
    }

    @Override
    public void cleanup() {
    }

    /**
     * scheduled runnable callback, called periodically
     */
    @Override
    public void run() {
        try {
            GrayUpgradeConfig grayUpgradeConf = (GrayUpgradeConfig) stormClusterState.get_gray_upgrade_conf(topologyId);

            // no upgrade request
            if (grayUpgradeConf == null) {
                LOG.debug("gray upgrade conf is null, skip...");
                return;
            }

            if (grayUpgradeConf.isCompleted() && !grayUpgradeConf.isRollback()) {
                LOG.debug("detected a complete upgrade, skip...");
                return;
            }

            if (grayUpgradeConf.isExpired() && !grayUpgradeConf.isRollback()) {
                LOG.info("detected an expired upgrade, completing...");
                // todo: should we check all task status?
                GrayUpgradeConfig.completeUpgrade(grayUpgradeConf);
                //stormClusterState.remove_gray_upgrade_info(topologyId);
                stormClusterState.set_gray_upgrade_conf(topologyId, grayUpgradeConf);
                stormClusterState.update_storm(topologyId, new StormStatus(StatusType.active));
                return;
            }

            // first time, set workers
            if (this.totalWorkers.size() == 0) {
                setTotalWorkers(tmContext);
            }

            // notify current upgrading workers to upgrade (again)
            Set<String> upgradingWorkers = Sets.newHashSet(stormClusterState.get_upgrading_workers(topologyId));
            if (upgradingWorkers.size() > 0) {
                LOG.info("Following workers are under upgrade:{}", upgradingWorkers);
                for (String worker : upgradingWorkers) {
                    notifyToUpgrade(worker);
                }
                return;
            }

            Set<String> upgradedWorkers = Sets.newHashSet(stormClusterState.get_upgraded_workers(topologyId));
            if (grayUpgradeConf.isRollback()) {
                LOG.info("Rollback has completed, removing upgrade info in zk and updating storm status...");
                // there's no way back after a rollback
                stormClusterState.remove_gray_upgrade_info(topologyId);
                stormClusterState.update_storm(topologyId, new StormStatus(StatusType.active));
                return;
            }

            if (isUpgradeCompleted(upgradedWorkers, totalWorkers)) {
                LOG.info("This upgraded has finished! Marking upgrade config as completed...");
                GrayUpgradeConfig.completeUpgrade(grayUpgradeConf);
                stormClusterState.set_gray_upgrade_conf(topologyId, grayUpgradeConf);
                //stormClusterState.remove_gray_upgrade_info(topologyId);
                stormClusterState.update_storm(topologyId, new StormStatus(StatusType.active));
                return;
            }

            // assign next batch of workers
            if (grayUpgradeConf.continueUpgrading()) {
                pickWorkersToUpgrade(grayUpgradeConf, upgradedWorkers);
            }

            // pause upgrading
            grayUpgradeConf.setContinueUpgrade(false);
            stormClusterState.set_gray_upgrade_conf(topologyId, grayUpgradeConf);
        } catch (Exception ex) {
            LOG.error("Failed to get upgrade config from zk, will abort this upgrade...", ex);
            recover();
        }
    }

    private void pickWorkersToUpgrade(GrayUpgradeConfig grayUpgradeConf, Set<String> upgradedWorkers) throws Exception {
        Set<String> remainingSlots = new HashSet<>();
        remainingSlots.addAll(this.totalWorkers);
        remainingSlots.removeAll(upgradedWorkers);

        int workerNum = grayUpgradeConf.getWorkerNum();
        String component = grayUpgradeConf.getComponent();
        Set<String> workers = grayUpgradeConf.getWorkers();
        TopologyContext topologyContext = tmContext.getContext();

        if (workers.size() > 0) {
            LOG.info("Upgrading specified workers:{}", workers);
            for (String worker : workers) {
                if (remainingSlots.contains(worker)) {
                    addUpgradingSlot(worker);
                } else {
                    LOG.warn("Worker {} is not in topology worker list or has been upgraded already, skip.", worker);
                }
            }
            // reset workers
            workers.clear();
        } else if (!StringUtils.isBlank(component)) {
            LOG.info("Upgrading workers of component:{}", component);
            List<Integer> tasks = topologyContext.getComponentTasks(component);
            if (tasks == null) {
                LOG.error("Failed to get tasks for component {}, maybe it's a wrong component name.", component);
                return;
            }

            Set<String> slots = new HashSet<>();
            for (Integer task : tasks) {
                String worker = this.taskToHostPort.get(task);
                if (worker != null && remainingSlots.contains(worker)) {
                    slots.add(worker);
                }
            }
            LOG.info("Available workers of component {}: {}", component, slots);
            pickUpgradingSlots(slots, workerNum > 0 ? workerNum : slots.size());
            // reset component
            if (workerNum == 0 || workerNum >= slots.size()) {
                grayUpgradeConf.setComponent(null);
            }
        } else if (workerNum > 0) {
            LOG.info("Upgrading workers at random");
            pickUpgradingSlots(remainingSlots, workerNum);
        }
    }

    private void pickUpgradingSlots(Set<String> remainingSlots, int n) throws Exception {
        // pick workers
        int i = 0;
        for (String remainingSlot : remainingSlots) {
            addUpgradingSlot(remainingSlot);
            i++;
            if (i == n) {
                break;
            }
        }
    }

    private void addUpgradingSlot(String worker) throws Exception {
        stormClusterState.add_upgrading_worker(topologyId, worker);
        notifyToUpgrade(worker);
    }

    private boolean isUpgradeCompleted(Collection<String> upgradedWorkers, Collection<String> allWorkers) {
        return upgradedWorkers.size() > 0 && upgradedWorkers.size() >= allWorkers.size();
    }

    private void notifyToUpgrade(String workerSlot) {
        int headTask = hostPortToTasks.get(workerSlot).iterator().next();
        LOG.info("notifying worker {} to upgrade(task {})...", workerSlot, headTask);
        //collector.emitDirect(headTask, Common.TOPOLOGY_MASTER_GRAY_UPGRADE_STREAM_ID, new Values("upgrade"));
    }

    private void setTotalWorkers(TopologyMasterContext tmContext) {
        Set<ResourceWorkerSlot> workerSlots = tmContext.getWorkerSet().get();
        int tmTaskId = tmContext.getTaskId();
        this.totalWorkers.clear();
        for (ResourceWorkerSlot workerSlot : workerSlots) {
            if (!workerSlot.getTasks().contains(tmTaskId)) {
                this.totalWorkers.add(workerSlot.getHostPort());
            }
        }
    }

    private void recover() {
        try {
            LOG.info("Removing upgrading info...");
            stormClusterState.remove_gray_upgrade_info(topologyId);

            LOG.info("Reset topology state to ACTIVE...");
            stormClusterState.update_storm(topologyId, new StormStatus(StatusType.active));
        } catch (Exception ex) {
            LOG.error("Failed to recover from upgrade", ex);
        }
    }
}
