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
package com.alibaba.jstorm.schedule.default_assign;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.IToplogyScheduler;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;

public class DefaultTopologyScheduler implements IToplogyScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyScheduler.class);

    private Map nimbusConf;

    @Override
    public void prepare(Map conf) {
        nimbusConf = conf;
    }

    /**
     * @@@ Here maybe exist one problem, some dead slots have been free
     * 
     * @param context
     */
    protected void freeUsed(TopologyAssignContext context) {
        Set<Integer> canFree = new HashSet<Integer>();
        canFree.addAll(context.getAllTaskIds());
        canFree.removeAll(context.getUnstoppedTaskIds());

        Map<String, SupervisorInfo> cluster = context.getCluster();
        Assignment oldAssigns = context.getOldAssignment();
        for (Integer task : canFree) {
            ResourceWorkerSlot worker = oldAssigns.getWorkerByTaskId(task);
            if (worker == null) {
                LOG.warn("When free rebalance resource, no ResourceAssignment of task " + task);
                continue;
            }

            SupervisorInfo supervisorInfo = cluster.get(worker.getNodeId());
            if (supervisorInfo == null) {
                continue;
            }
            supervisorInfo.getAvailableWorkerPorts().add(worker.getPort());
        }
    }

    private Set<Integer> getNeedAssignTasks(DefaultTopologyAssignContext context) {
        Set<Integer> needAssign = new HashSet<Integer>();

        int assignType = context.getAssignType();
        if (assignType == TopologyAssignContext.ASSIGN_TYPE_NEW) {
            needAssign.addAll(context.getAllTaskIds());
        } else if (assignType == TopologyAssignContext.ASSIGN_TYPE_REBALANCE) {
            needAssign.addAll(context.getAllTaskIds());
            needAssign.removeAll(context.getUnstoppedTaskIds());
        } else { // ASSIGN_TYPE_MONITOR
            Set<Integer> deadTasks = context.getDeadTaskIds();
            needAssign.addAll(deadTasks);
        }

        return needAssign;
    }

    /**
     * Get the task Map which the task is alive and will be kept Only when type is ASSIGN_TYPE_MONITOR, it is valid
     * 
     * @param defaultContext
     * @param needAssigns
     * @return
     */
    public Set<ResourceWorkerSlot> getKeepAssign(DefaultTopologyAssignContext defaultContext, Set<Integer> needAssigns) {

        Set<Integer> keepAssignIds = new HashSet<Integer>();
        keepAssignIds.addAll(defaultContext.getAllTaskIds());
        keepAssignIds.removeAll(defaultContext.getUnstoppedTaskIds());
        keepAssignIds.removeAll(needAssigns);
        Set<ResourceWorkerSlot> keeps = new HashSet<ResourceWorkerSlot>();
        if (keepAssignIds.isEmpty()) {
            return keeps;
        }

        Assignment oldAssignment = defaultContext.getOldAssignment();
        if (oldAssignment == null) {
            return keeps;
        }
        keeps.addAll(defaultContext.getOldWorkers());
        for (ResourceWorkerSlot worker : defaultContext.getOldWorkers()) {
            for (Integer task : worker.getTasks()) {
                if (!keepAssignIds.contains(task)) {
                    keeps.remove(worker);
                    break;
                }
            }
        }
        return keeps;
    }

    @Override
    public Set<ResourceWorkerSlot> assignTasks(TopologyAssignContext context) throws FailedAssignTopologyException {

        int assignType = context.getAssignType();
        if (TopologyAssignContext.isAssignTypeValid(assignType) == false) {
            throw new FailedAssignTopologyException("Invalide Assign Type " + assignType);
        }

        DefaultTopologyAssignContext defaultContext = new DefaultTopologyAssignContext(context);
        if (assignType == TopologyAssignContext.ASSIGN_TYPE_REBALANCE) {
            /**
             * Mark all current assigned worker as available. Current assignment will be restored in task scheduler.
             */
            freeUsed(defaultContext);
        }
        LOG.info("Dead tasks:" + defaultContext.getDeadTaskIds());
        LOG.info("Unstopped tasks:" + defaultContext.getUnstoppedTaskIds());

        Set<Integer> needAssignTasks = getNeedAssignTasks(defaultContext);

        Set<ResourceWorkerSlot> keepAssigns = getKeepAssign(defaultContext, needAssignTasks);

        // please use tree map to make task sequence
        Set<ResourceWorkerSlot> ret = new HashSet<ResourceWorkerSlot>();
        ret.addAll(keepAssigns);
        ret.addAll(defaultContext.getUnstoppedWorkers());

        int allocWorkerNum = defaultContext.getTotalWorkerNum() - defaultContext.getUnstoppedWorkerNum() - keepAssigns.size();
        LOG.info("allocWorkerNum=" + allocWorkerNum + ", totalWorkerNum=" + defaultContext.getTotalWorkerNum() + ", keepWorkerNum=" + keepAssigns.size());

        if (allocWorkerNum <= 0) {
            LOG.warn("Don't need assign workers, all workers are fine " + defaultContext.toDetailString());
            throw new FailedAssignTopologyException("Don't need assign worker, all workers are fine ");
        }

        List<ResourceWorkerSlot> availableWorkers = WorkerScheduler.getInstance().getAvailableWorkers(defaultContext, needAssignTasks, allocWorkerNum);
        TaskScheduler taskScheduler = new TaskScheduler(defaultContext, needAssignTasks, availableWorkers);
        Set<ResourceWorkerSlot> assignment = new HashSet<ResourceWorkerSlot>(taskScheduler.assign());

        //setting worker's memory for TM
        int topologyMasterId = defaultContext.getTopologyMasterTaskId();
        Long tmWorkerMem = ConfigExtension.getMemSizePerTopologyMasterWorker(defaultContext.getStormConf());
        if (tmWorkerMem != null){
            for (ResourceWorkerSlot resourceWorkerSlot : assignment){
                if (resourceWorkerSlot.getTasks().contains(topologyMasterId)){
                    resourceWorkerSlot.setMemSize(tmWorkerMem);
                }
            }
        }

        ret.addAll(assignment);

        LOG.info("Keep Alive slots:" + keepAssigns);
        LOG.info("Unstopped slots:" + defaultContext.getUnstoppedWorkers());
        LOG.info("New assign slots:" + assignment);

        return ret;
    }

}
