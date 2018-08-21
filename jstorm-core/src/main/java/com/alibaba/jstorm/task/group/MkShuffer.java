/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task.group;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.messaging.IConnection;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.DisruptorQueue;

public class MkShuffer {
    private static final Logger LOG = LoggerFactory.getLogger(MkShuffer.class);

    private final WorkerData workerData;
    private final String sourceComponent;
    private final String targetComponent;

    private List<Integer> localWorkerTasks;
    private int localWorkerTaskSize;
    private AtomicInteger localWorkerTaskIndex = new AtomicInteger(0);
    private List<Integer> outWorkerTasks = new ArrayList<>();
    private int outWorkerTaskSize;
    private AtomicInteger outWorkerTaskIndex = new AtomicInteger(0);

    private Map<Integer, WorkerSlot> taskNodePort;
    private Map<WorkerSlot, IConnection> nodePortToSocket;
    private Set<Integer> oldLocalNodeTasks;
    private float loadMark;
    private boolean isInterShuffle;

    private IntervalCheck intervalCheck;

    public void refreshTasks() {
        isInterShuffle = ConfigExtension.getShuffleEnableInterPath(workerData.getStormConf());
        loadMark = (float) ConfigExtension.getShuffleInterLoadMark(workerData.getStormConf());

        Set<Integer> localNodeTasks = workerData.getLocalNodeTasks();
        if (oldLocalNodeTasks != null && oldLocalNodeTasks.equals(localNodeTasks)) {
            return;
        } else {
            oldLocalNodeTasks = new HashSet<>(localNodeTasks);
        }

        taskNodePort = workerData.getTaskToNodePort();
        nodePortToSocket = workerData.getNodePortToSocket();

        Set<Integer> localWorkerTaskSet = workerData.getTaskIds();
        Map<String, List<Integer>> componentTasks = workerData.getComponentToSortedTasks();
        Set<Integer> sourceTasks = JStormUtils.listToSet(componentTasks.get(sourceComponent));
        Set<Integer> targetTasks = JStormUtils.listToSet(componentTasks.get(targetComponent));

        ArrayList<Integer> localWorkerTasksTmp = new ArrayList<>();
        ArrayList<Integer> localNodeTasksTmp = new ArrayList<>();
        ArrayList<Integer> otherNodeTasksTmp = new ArrayList<>();
        for (Integer tasks : targetTasks) {
            if (localWorkerTaskSet.contains(tasks)) {
                localWorkerTasksTmp.add(tasks);
            } else if (localNodeTasks.contains(tasks)) {
                localNodeTasksTmp.add(tasks);
            } else {
                otherNodeTasksTmp.add(tasks);
            }
        }

        if (this.localWorkerTasks == null) {
            this.localWorkerTasks = localWorkerTasksTmp;
            localWorkerTaskSize = this.localWorkerTasks.size();
        }

        if (!isInterShuffle) {
            localWorkerTaskSize = 0;
            outWorkerTasks = JStormUtils.mk_list(targetTasks);
            outWorkerTaskSize = outWorkerTasks.size();
            return;
        }

        // the left logic is when isInterShuffle is true

        Set<String> sourceHosts = new HashSet<>();
        Set<String> targetHosts = new HashSet<>();
        for (Entry<Integer, WorkerSlot> entry : taskNodePort.entrySet()) {
            Integer task = entry.getKey();
            WorkerSlot workerSlot = entry.getValue();
            String host = workerSlot.getNodeId();
            if (sourceTasks.contains(task)) {
                sourceHosts.add(host);
            } else if (targetTasks.contains(task)) {
                targetHosts.add(host);
            }
        }
        LOG.info("{} hosts {} tasks {}, {} hosts {} tasks {}", sourceComponent, sourceHosts, sourceTasks,
                targetComponent, targetHosts, targetTasks);

        double localNodePriority = 2.0;
        if (targetHosts.equals(sourceHosts) && targetHosts.size() > 0) {
            // due to every node's has the source, double the priority
            localNodePriority *= 2;
        }
        if (localWorkerTasksTmp.size() != 0) {
            //due to current worker will consume much cpu, so reduce priority
            localNodePriority /= 2;
        }

        ArrayList<Integer> outWorkerTasksTmp = new ArrayList<>();
        outWorkerTasksTmp.addAll(localNodeTasksTmp);
        outWorkerTasksTmp.addAll(otherNodeTasksTmp);
        for (int i = 1; i < localNodePriority; i++) {
            outWorkerTasksTmp.addAll(localNodeTasksTmp);
        }

        this.outWorkerTasks = outWorkerTasksTmp;
        outWorkerTaskSize = outWorkerTasks.size();

        LOG.info("Source:{}, target:{}, localTasks:{}, outTasks:{}", sourceComponent, targetComponent, localWorkerTasks,
                outWorkerTasks);
    }

    public MkShuffer(String sourceComponent, String targetComponent, WorkerData workerData) {
        this.workerData = workerData;
        this.sourceComponent = sourceComponent;
        this.targetComponent = targetComponent;

        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(60 * 2);

        refreshTasks();

    }

    private boolean isOutboundTaskAvailable(int taskId) {
        boolean ret = false;
        DisruptorQueue targetQueue = workerData.getInnerTaskTransfer().get(taskId);

        if (targetQueue != null) {
            float queueLoadRatio = targetQueue.pctFull();
            if (queueLoadRatio < loadMark) {
                ret = true;
            }
        } else {
            WorkerSlot slot = taskNodePort.get(taskId);
            if (slot != null) {
                IConnection connection = nodePortToSocket.get(slot);
                if (connection != null) {
                    ret = connection.available(taskId);
                }
            }
        }

        if (!ret) {
            LOG.debug("taskId:{} is unavailable", taskId);
        }

        return ret;
    }

    protected Integer getInerShuffle() {
        if (!isInterShuffle || localWorkerTaskSize == 0) {
            return null;
        }

        for (int i = 0; i < localWorkerTaskSize; i++) {

            int index = localWorkerTaskIndex.incrementAndGet();
            if (index >= localWorkerTaskSize) {
                index = 0;
                localWorkerTaskIndex.set(0);
            }
            int ret = localWorkerTasks.get(index);
            if (isOutboundTaskAvailable(ret)) {
                return ret;
            }
        }

        return null;
    }

    protected Integer getOuterShuffle() {
        for (int i = 0; i < outWorkerTaskSize; i++) {

            int index = outWorkerTaskIndex.incrementAndGet();
            if (index >= outWorkerTaskSize) {
                index = 0;
                outWorkerTaskIndex.set(0);
            }

            int ret = outWorkerTasks.get(index);
            if (isOutboundTaskAvailable(ret)) {
                return ret;
            }
        }

        return null;
    }

    protected Integer getBadShuffle() {
        LOG.debug("No available task");
        if (localWorkerTaskSize > 0) {
            int index = localWorkerTaskIndex.incrementAndGet();
            if (index >= localWorkerTaskSize) {
                index = 0;
                localWorkerTaskIndex.set(0);
            }
            return localWorkerTasks.get(index);
        } else {
            int index = outWorkerTaskIndex.incrementAndGet();
            if (index >= outWorkerTaskSize) {
                index = 0;
                outWorkerTaskIndex.set(0);
            }
            return outWorkerTasks.get(index);
        }

    }

    @SuppressWarnings("unused")
    public List<Integer> grouper(List<Object> values) {
        Integer ret = getInerShuffle();
        if (ret != null) {
            return JStormUtils.mk_list(ret);
        }

        if (intervalCheck.check()) {
            refreshTasks();
        }

        ret = getOuterShuffle();
        if (ret != null) {
            return JStormUtils.mk_list(ret);
        }
        return JStormUtils.mk_list(getBadShuffle());
    }
}
