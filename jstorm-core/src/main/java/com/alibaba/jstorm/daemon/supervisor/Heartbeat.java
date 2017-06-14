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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.daemon.worker.LocalAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * supervisor heartbeat, just write SupervisorInfo to ZK
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
class Heartbeat extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(Heartbeat.class);

    private Map<Object, Object> conf;
    private StormClusterState stormClusterState;
    private String supervisorId;
    private String myHostName;
    private final int startTime;
    private final int frequency;

    private SupervisorInfo supervisorInfo;
    private AtomicBoolean hbUpdateTrigger;
    protected volatile HealthStatus healthStatus;

    private LocalState localState;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Heartbeat(Map conf, StormClusterState stormClusterState, String supervisorId,
                     LocalState localState) {
        String myHostName = JStormServerUtils.getHostName(conf);
        this.stormClusterState = stormClusterState;
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.myHostName = myHostName;
        this.startTime = TimeUtils.current_time_secs();
        this.frequency = JStormUtils.parseInt(conf.get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));
        this.hbUpdateTrigger = new AtomicBoolean(true);
        this.localState = localState;
        this.healthStatus = HealthStatus.INFO;

        initSupervisorInfo(conf);

        LOG.info("Successfully inited supervisor heartbeat thread, " + supervisorInfo);
    }

    private void initSupervisorInfo(Map conf) {
        Set<Integer> portList = JStormUtils.getDefaultSupervisorPortList(conf);
        if (!StormConfig.local_mode(conf)) {
            try {
                boolean isLocalIP = myHostName.equals("127.0.0.1") || myHostName.equals("localhost");
                if (isLocalIP) {
                    throw new Exception("the hostname supervisor got is localhost");
                }
            } catch (Exception e1) {
                LOG.error("get supervisor host error!", e1);
                throw new RuntimeException(e1);
            }
            supervisorInfo = new SupervisorInfo(myHostName, supervisorId, portList, conf);
        } else {
            supervisorInfo = new SupervisorInfo(myHostName, supervisorId, portList, conf);
        }

        supervisorInfo.setVersion(Utils.getVersion());
        String buildTs = Utils.getBuildTime();
        supervisorInfo.setBuildTs(buildTs);
        LOG.info("jstorm version:{}, build ts:{}", supervisorInfo.getVersion(), supervisorInfo.getBuildTs());
    }

    @SuppressWarnings("unchecked")
    public void update() {
        supervisorInfo.setTimeSecs(TimeUtils.current_time_secs());
        supervisorInfo.setUptimeSecs(TimeUtils.current_time_secs() - startTime);

        updateSupervisorInfo();

        try {
            stormClusterState.supervisor_heartbeat(supervisorId, supervisorInfo);
        } catch (Exception e) {
            LOG.error("Failed to update SupervisorInfo to ZK", e);
        }
    }

    private void updateSupervisorInfo() {
        Set<Integer> portList = calculateCurrentPortList();
        LOG.debug("portList : {}", portList);
        supervisorInfo.setWorkerPorts(portList);
    }

    public void updateHealthStatus(HealthStatus status) {
        this.healthStatus = status;
    }

    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    @Override
    public Object getResult() {
        return frequency;
    }

    @Override
    public void run() {
        boolean updateHb = hbUpdateTrigger.getAndSet(false);
        if (updateHb) {
            update();
        }
    }

    public int getStartTime() {
        return startTime;
    }

    public SupervisorInfo getSupervisorInfo() {
        return supervisorInfo;
    }

    public void updateHbTrigger(boolean update) {
        hbUpdateTrigger.set(update);
    }

    private Set<Integer> calculateCurrentPortList() {
        Set<Integer> defaultPortList = JStormUtils.getDefaultSupervisorPortList(conf);

        Set<Integer> usedList;
        try {
            usedList = getLocalAssignmentPortList();
        } catch (IOException e) {
            supervisorInfo.setErrorMessage(null);
            return defaultPortList;
        }

        int availablePortNum = calculateAvailablePortNumber(defaultPortList, usedList);

        if (availablePortNum >= (defaultPortList.size() - usedList.size())) {
            supervisorInfo.setErrorMessage(null);
            return defaultPortList;
        } else {
            List<Integer> freePortList = new ArrayList<>(defaultPortList);
            freePortList.removeAll(usedList);
            Collections.sort(freePortList);
            Set<Integer> portList = new HashSet<>(usedList);
            for (int i = 1; i <= availablePortNum; i++) {
                portList.add(freePortList.get(i));
            }
            supervisorInfo.setErrorMessage("Supervisor is lack of resources, " +
                                          "reduce the number of workers from " + defaultPortList.size() +
                                          " to " + (usedList.size() + availablePortNum));
            return portList;
        }
    }

    private int calculateAvailablePortNumber(Set<Integer> defaultList, Set<Integer> usedList) {
        if (healthStatus.isMoreSeriousThan(HealthStatus.WARN)) {
            LOG.warn("Due to no enough resource, limit supervisor's ports and block scheduling");
            // set free port number to zero
            return 0;
        }

        int vcores = JStormUtils.getNumProcessors();
        double cpuUsage = JStormUtils.getTotalCpuUsage();

        // do not adjust port list if match the following conditions
        if (cpuUsage <= 0.0  // non-linux,
            || vcores <= 4   // machine configuration is too low
            || !ConfigExtension.isSupervisorEnableAutoAdjustSlots(conf) // auto adjust is disabled
                ) {
            return defaultList.size() - usedList.size();
        }

        long freeMemory = JStormUtils.getFreePhysicalMem() * 1024L;     // in Byte
        long reserveMemory = ConfigExtension.getStormMachineReserveMem(conf);
        Long availablePhysicalMemorySize = freeMemory - reserveMemory;
        int reserveCpuUsage = ConfigExtension.getStormMachineReserveCpuPercent(conf);

        if (availablePhysicalMemorySize < 0 || cpuUsage + reserveCpuUsage > 100D) {
            // memory is not enough, or cpu is not enough, set free ports number to zero
            return 0;
        } else {
            int availableCpuNum = (int) Math.round((100 - cpuUsage) / 100 * vcores);
            return JStormUtils.getSupervisorPortNum(conf, availableCpuNum, availablePhysicalMemorySize);
        }
    }

    private Set<Integer> getLocalAssignmentPortList() throws IOException {
        Map<Integer, LocalAssignment> localAssignment;
        try {
            localAssignment = (Map<Integer, LocalAssignment>) localState.get(Common.LS_LOCAL_ASSIGNMENTS);
        } catch (IOException e) {
            LOG.error("get LS_LOCAL_ASSIGNMENTS of localState failed .");
            throw e;
        }
        if (localAssignment == null) {
            return new HashSet<>();
        }
        return localAssignment.keySet();
    }
}
