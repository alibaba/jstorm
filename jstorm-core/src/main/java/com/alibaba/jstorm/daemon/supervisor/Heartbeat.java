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
 * supervisor Heartbeat, just write SupervisorInfo to ZK
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
class Heartbeat extends RunnableCallback {

    private static Logger LOG = LoggerFactory.getLogger(Heartbeat.class);

    private static final int CPU_THREADHOLD = 4;
    private static final long MEM_THREADHOLD = 8 * JStormUtils.SIZE_1_G;

    private Map<Object, Object> conf;

    private StormClusterState stormClusterState;

    private String supervisorId;

    private String myHostName;

    private final int startTime;

    private final int frequence;

    private SupervisorInfo supervisorInfo;

    private AtomicBoolean hbUpdateTrigger;
    //protected HealthStatus oldHealthStatus;
    //protected  volatile HealthStatus healthStatus;
    protected MachineCheckStatus oldCheckStatus;

    protected volatile MachineCheckStatus checkStatus;

    private LocalState localState;

    /**
     * @param conf
     * @param stormClusterState
     * @param supervisorId
     * @param status
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Heartbeat(Map conf, StormClusterState stormClusterState, String supervisorId, LocalState localState,
                     MachineCheckStatus status) {

        String myHostName = JStormServerUtils.getHostName(conf);

        this.stormClusterState = stormClusterState;
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.myHostName = myHostName;
        this.startTime = TimeUtils.current_time_secs();
        this.frequence = JStormUtils.parseInt(conf.get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));
        this.hbUpdateTrigger = new AtomicBoolean(true);
        this.localState = localState;
        this.checkStatus = status;
        oldCheckStatus = new MachineCheckStatus();
        oldCheckStatus.SetType(this.checkStatus.getType());

        initSupervisorInfo(conf);

        LOG.info("Successfully init supervisor heartbeat thread, " + supervisorInfo);
    }

    private void initSupervisorInfo(Map conf) {
        List<Integer> portList = JStormUtils.getSupervisorPortList(conf);

        if (!StormConfig.local_mode(conf)) {
            try {

                boolean isLocalIP = myHostName.equals("127.0.0.1") || myHostName.equals("localhost");
                if (isLocalIP) {
                    throw new Exception("the hostname which  supervisor get is localhost");
                }

            } catch (Exception e1) {
                LOG.error("get supervisor host error!", e1);
                throw new RuntimeException(e1);
            }
            Set<Integer> ports = JStormUtils.listToSet(portList);
            supervisorInfo = new SupervisorInfo(myHostName, supervisorId, ports, conf);
        } else {
            Set<Integer> ports = JStormUtils.listToSet(portList);
            supervisorInfo = new SupervisorInfo(myHostName, supervisorId, ports, conf);
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

        if (checkStatus.getType() == MachineCheckStatus.StatusType.warning
                || checkStatus.getType() == MachineCheckStatus.StatusType.error
                || checkStatus.getType() == MachineCheckStatus.StatusType.panic) {
            Set<Integer> ports = new HashSet<Integer>();
            supervisorInfo.setWorkerPorts(ports);
            if (!checkStatus.equals(oldCheckStatus)) {
                LOG.warn("due to no enough resource, limit supervisor's ports and block scheduling");
                oldCheckStatus.SetType(checkStatus.getType());
            }
        }

        try {
            stormClusterState.supervisor_heartbeat(supervisorId, supervisorInfo);
        } catch (Exception e) {
            LOG.error("Failed to update SupervisorInfo to ZK", e);

        }
    }

    private void updateSupervisorInfo() {
        List<Integer> portList = calculatorAvailablePorts();
        LOG.debug("portList : {}", portList);
        Set<Integer> ports = JStormUtils.listToSet(portList);
        supervisorInfo.setWorkerPorts(ports);
    }

    public MachineCheckStatus getCheckStatus() {
        return checkStatus;
    }

    @Override
    public Object getResult() {
        return frequence;
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

    private List<Integer> calculatorAvailablePorts() {

        List<Integer> defaultPortList = JStormUtils.getSupervisorPortList(conf);

        double cpuUsage = JStormUtils.getTotalCpuUsage();
        int reserveCpuUsage = ConfigExtension.getStormMachineReserveCpuPercent(conf);

        if (cpuUsage <= 0.0 || !ConfigExtension.isSupervisorEnableAutoAdjustSlots(conf)) {
            return defaultPortList;
        }
        long freeMemory = JStormUtils.getFreePhysicalMem() * 1024L;

        long reserveMemory = ConfigExtension.getStormMachineReserveMem(conf);

        if (freeMemory < reserveMemory) {
            List<Integer> list = null;
            try {
                list = getLocalAssignmentPortList();
            } catch (IOException e) {
                return defaultPortList;
            }
            if (list == null)
                return new ArrayList<Integer>();
            return list;
        }


        if (cpuUsage > (100D - reserveCpuUsage)) {
            List<Integer> list = null;
            try {
                list = getLocalAssignmentPortList();
            } catch (IOException e) {
                return defaultPortList;
            }
            if (list == null)
                return new ArrayList<Integer>();
            return list;
        }

        Long conversionAvailableCpuNum = Math.round((100 - cpuUsage) / 100 * JStormUtils.getNumProcessors());

        Long availablePhysicalMemorySize = freeMemory - reserveMemory;

        int portNum = JStormUtils.getSupervisorPortNum(
                conf, conversionAvailableCpuNum.intValue(), availablePhysicalMemorySize, true);

        List<Integer> portList = new ArrayList<>();
        portList.addAll(defaultPortList);

        List<Integer> usedList = null;
        try {
            usedList = getLocalAssignmentPortList();
        } catch (Exception e) {
            return defaultPortList;
        }
        portList.removeAll(usedList);

        Collections.sort(portList);
        //Collections.sort(usedList);

        if (portNum >= portList.size()) {
            return defaultPortList;
        } else {
            List<Integer> reportPortList = new ArrayList<Integer>();
            reportPortList.addAll(usedList);
            for (int i = 1; i <= portNum; i++) {
                reportPortList.add(portList.get(i));
            }
            return reportPortList;
        }
    }

    private List<Integer> getLocalAssignmentPortList() throws IOException {

        Map<Integer, LocalAssignment> localAssignment = null;
        try {
            localAssignment = (Map<Integer, LocalAssignment>) localState.get(Common.LS_LOCAL_ASSIGNMENTS);
        } catch (IOException e) {
            LOG.error("get LS_LOCAL_ASSIGNMENTS of localState failed .");
            throw e;
        }
        if (localAssignment == null) {
            return null;
        }
        return JStormUtils.mk_list(localAssignment.keySet());
    }
}
