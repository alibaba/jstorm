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

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.callback.impl.*;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Status changing
 *
 * @author version1: lixin version2: Longda
 */
public class StatusTransition {
    private final static Logger LOG = LoggerFactory.getLogger(StatusTransition.class);

    private NimbusData data;
    private Map<String, Object> topologyLocks = new ConcurrentHashMap<>();

    public StatusTransition(NimbusData data) {
        this.data = data;
    }

    public <T> void transition(String topologyId, boolean errorOnNoTransition, StatusType changeStatus, T... args)
            throws Exception {
        // lock outside
        Object lock = topologyLocks.get(topologyId);
        if (lock == null) {
            lock = new Object();
            topologyLocks.put(topologyId, lock);
        }

        if (data.getIsShutdown().get()) {
            LOG.info("Nimbus is shutting down, skip this event " + topologyId + "->" + changeStatus);
            return;
        }

        synchronized (lock) {
            transitionLock(topologyId, errorOnNoTransition, changeStatus, args);
            // update the lock times
            topologyLocks.put(topologyId, lock);
        }
    }

    /**
     * Changing status
     *
     * @param args -- will be used in the status changing callback
     */
    public <T> void transitionLock(String topologyId, boolean errorOnNoTransition, StatusType changeStatus, T... args)
            throws Exception {
        // get ZK's topology node's data, which is StormBase
        StormBase stormbase = data.getStormClusterState().storm_base(topologyId, null);
        if (stormbase == null) {
            LOG.error("Cannot apply event: changing status " + topologyId + " -> " + changeStatus.getStatus() +
                    ", cause: failed to get StormBase from ZK");
            return;
        }

        StormStatus currentStatus = stormbase.getStatus();
        if (currentStatus == null) {
            LOG.error("Cannot apply event: changing status " + topologyId + " -> " + changeStatus.getStatus() +
                    ", cause: topologyStatus is null in ZK");
            return;
        }

        // <currentStatus, Map<changingStatus, callback>>
        Map<StatusType, Map<StatusType, Callback>> callbackMap = stateTransitions(topologyId, currentStatus);

        // get current changingCallbacks
        Map<StatusType, Callback> changingCallbacks = callbackMap.get(currentStatus.getStatusType());

        if (changingCallbacks == null || !changingCallbacks.containsKey(changeStatus) ||
                changingCallbacks.get(changeStatus) == null) {
            String msg = "No transition for event: changing status:" + changeStatus.getStatus() +
                    ", current status: " + currentStatus.getStatusType() + ", topology-id: " + topologyId;
            LOG.info(msg);
            if (errorOnNoTransition) {
                throw new RuntimeException(msg);
            }
            return;
        }

        Callback callback = changingCallbacks.get(changeStatus);

        Object obj = callback.execute(args);
        if (obj != null && obj instanceof StormStatus) {
            StormStatus newStatus = (StormStatus) obj;
            // update status to ZK
            data.getStormClusterState().update_storm(topologyId, newStatus);
            LOG.info("Successfully updated " + topologyId + " to status " + newStatus);
        }

        LOG.info("Successfully apply event: changing status " + topologyId + " -> " + changeStatus.getStatus());
    }

    /**
     * generate status changing map
     *
     * @param topologyId    topology id
     * @param currentStatus current topology status
     * @return Map[StatusType, Map[StatusType, Callback]] means Map[currentStatus, Map[changingStatus, Callback]]
     */

    private Map<StatusType, Map<StatusType, Callback>> stateTransitions(String topologyId, StormStatus currentStatus) {

        /**
         *
         * 1. Status: this status will be stored in ZK killed/inactive/active/rebalancing 2. action:
         *
         * monitor -- every Config.NIMBUS_MONITOR_FREQ_SECS seconds will trigger this only valid when current status is active inactivate -- client will trigger
         * this action, only valid when current status is active activate -- client will trigger this action only valid when current status is inactive startup
         * -- when nimbus startup, it will trigger this action only valid when current status is killed/rebalancing kill -- client kill topology will trigger
         * this action, only valid when current status is active/inactive/killed remove -- 30 seconds after client submit kill command, it will do this action,
         * only valid when current status is killed rebalance -- client submit rebalance command, only valid when current status is active/deactive do_rebalance
         * -- 30 seconds after client submit rebalance command, it will do this action, only valid when current status is rebalance
         */

        Map<StatusType, Map<StatusType, Callback>> rtn = new HashMap<>();

        // current status is active
        Map<StatusType, Callback> activeMap = new HashMap<>();
        activeMap.put(StatusType.monitor, new ReassignTransitionCallback(data, topologyId));
        activeMap.put(StatusType.inactivate, new InactiveTransitionCallback());
        activeMap.put(StatusType.startup, null);
        activeMap.put(StatusType.activate, null);
        activeMap.put(StatusType.kill, new KillTransitionCallback(data, topologyId));
        activeMap.put(StatusType.remove, null);
        activeMap.put(StatusType.rebalance, new RebalanceTransitionCallback(data, topologyId, currentStatus));
        activeMap.put(StatusType.do_rebalance, null);
        activeMap.put(StatusType.done_rebalance, null);
        activeMap.put(StatusType.update_topology, new UpdateTopologyTransitionCallback(data, topologyId, currentStatus));

        rtn.put(StatusType.active, activeMap);

        // current status is inactive
        Map<StatusType, Callback> inactiveMap = new HashMap<>();

        inactiveMap.put(StatusType.monitor, new ReassignTransitionCallback(data, topologyId, new StormStatus(StatusType.inactive)));
        inactiveMap.put(StatusType.inactivate, null);
        inactiveMap.put(StatusType.startup, null);
        inactiveMap.put(StatusType.activate, new ActiveTransitionCallback());
        inactiveMap.put(StatusType.kill, new KillTransitionCallback(data, topologyId));
        inactiveMap.put(StatusType.remove, null);
        inactiveMap.put(StatusType.rebalance, new RebalanceTransitionCallback(data, topologyId, currentStatus));
        inactiveMap.put(StatusType.do_rebalance, null);
        inactiveMap.put(StatusType.done_rebalance, null);
        inactiveMap.put(StatusType.update_topology, null);

        rtn.put(StatusType.inactive, inactiveMap);

        // current status is killed
        Map<StatusType, Callback> killedMap = new HashMap<>();

        killedMap.put(StatusType.monitor, null);
        killedMap.put(StatusType.inactivate, null);
        killedMap.put(StatusType.startup, new KillTransitionCallback(data, topologyId));
        killedMap.put(StatusType.activate, null);
        killedMap.put(StatusType.kill, new KillTransitionCallback(data, topologyId));
        killedMap.put(StatusType.remove, new RemoveTransitionCallback(data, topologyId));
        killedMap.put(StatusType.rebalance, null);
        killedMap.put(StatusType.do_rebalance, null);
        killedMap.put(StatusType.done_rebalance, null);
        killedMap.put(StatusType.update_topology, null);
        rtn.put(StatusType.killed, killedMap);

        // current status is under rebalancing
        Map<StatusType, Callback> rebalancingMap = new HashMap<>();

        StatusType rebalanceOldStatus = StatusType.active;
        if (currentStatus.getOldStatus() != null) {
            rebalanceOldStatus = currentStatus.getOldStatus().getStatusType();
            // fix double rebalance, make the status always as rebalancing
            if (rebalanceOldStatus == StatusType.rebalancing) {
                rebalanceOldStatus = StatusType.active;
            }
        }

        rebalancingMap.put(StatusType.monitor, null);
        rebalancingMap.put(StatusType.inactivate, null);
        rebalancingMap.put(StatusType.startup, new RebalanceTransitionCallback(data, topologyId, new StormStatus(rebalanceOldStatus)));
        rebalancingMap.put(StatusType.activate, null);
        rebalancingMap.put(StatusType.kill, null);
        rebalancingMap.put(StatusType.remove, null);
        rebalancingMap.put(StatusType.rebalance, new RebalanceTransitionCallback(data, topologyId, currentStatus));
        rebalancingMap.put(StatusType.do_rebalance, new DoRebalanceTransitionCallback(data, topologyId, new StormStatus(rebalanceOldStatus)));
        rebalancingMap.put(StatusType.done_rebalance, new DoneRebalanceTransitionCallback(data, topologyId));
        rebalancingMap.put(StatusType.update_topology, null);
        rtn.put(StatusType.rebalancing, rebalancingMap);

        Map<StatusType, Callback> upgradingMap = new HashMap<>();
        upgradingMap.put(StatusType.monitor, new RollbackTransitionCallback(data, topologyId));
        upgradingMap.put(StatusType.inactivate, null);
        upgradingMap.put(StatusType.startup, null);
        upgradingMap.put(StatusType.activate, null);
        upgradingMap.put(StatusType.kill, new KillTransitionCallback(data, topologyId));
        upgradingMap.put(StatusType.remove, new RemoveTransitionCallback(data, topologyId));
        upgradingMap.put(StatusType.rebalance, null);
        upgradingMap.put(StatusType.do_rebalance, null);
        upgradingMap.put(StatusType.done_rebalance, null);
        upgradingMap.put(StatusType.update_topology, null);
        upgradingMap.put(StatusType.upgrading, null);
        rtn.put(StatusType.upgrading, upgradingMap);

        return rtn;
    }

}
