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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.client.WorkerAssignment;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * allocation unit for a worker
 */
public class ResourceWorkerSlot extends WorkerSlot implements Serializable {
    private static final long serialVersionUID = 9138386287559932411L;
    public static Logger LOG = LoggerFactory.getLogger(ResourceWorkerSlot.class);

    private String hostname;
    private long memSize;
    private int cpu;
    private Set<Integer> tasks;
    private String jvm;

    public ResourceWorkerSlot() {
    }

    public ResourceWorkerSlot(String supervisorId, Integer port) {
        super(supervisorId, port);
    }

    public ResourceWorkerSlot(WorkerAssignment worker, Map<String, List<Integer>> componentToTask) {
        super(worker.getNodeId(), worker.getPort());
        this.hostname = worker.getHostName();
        this.tasks = new HashSet<>();
        this.cpu = worker.getCpu();
        this.memSize = worker.getMem();
        this.jvm = worker.getJvm();
        for (Entry<String, Integer> entry : worker.getComponentToNum().entrySet()) {
            List<Integer> tasks = componentToTask.get(entry.getKey());
            if (tasks == null || tasks.size() == 0) {
                continue;
            }
            int num = Math.min(tasks.size(), entry.getValue().intValue());
            List<Integer> cTasks = new ArrayList<>();
            cTasks.addAll(tasks.subList(0, num));
            this.tasks.addAll(cTasks);
            tasks.removeAll(cTasks);
        }
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Set<Integer> getTasks() {
        return tasks;
    }

    public void setTasks(Set<Integer> tasks) {
        this.tasks = tasks;
    }

    public String getJvm() {
        return jvm;
    }

    public void setJvm(String jvm) {
        this.jvm = jvm;
    }

    public long getMemSize() {
        return memSize;
    }

    public void setMemSize(long memSize) {
        this.memSize = memSize;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public boolean compareToUserDefineWorker(WorkerAssignment worker, Map<Integer, String> taskToComponent) {
        int cpu = worker.getCpu();
        if (cpu != 0 && this.cpu != cpu)
            return false;
        long mem = worker.getMem();
        if (mem != 0 && this.memSize != mem)
            return false;
        String jvm = worker.getJvm();
        if (jvm != null && !jvm.equals(this.jvm))
            return false;
        String hostName = worker.getHostName();
        if (!NetWorkUtils.equals(hostname, hostName))
            return false;
        int port = worker.getPort();
        if (port != 0 && port != this.getPort())
            return false;
        Map<String, Integer> componentToNum = worker.getComponentToNum();
        Map<String, Integer> myComponentToNum = new HashMap<>();
        for (Integer task : tasks) {
            String component = taskToComponent.get(task);
            Integer i = myComponentToNum.get(component);
            if (i == null) {
                i = 0;
            }
            myComponentToNum.put(component, ++i);
        }

        return myComponentToNum.equals(componentToNum);
    }

    public String getHostPort() {
        return hostname + ":" + getPort();
    }
}
