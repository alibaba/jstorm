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
package com.alibaba.jstorm.ui.model;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ClusterEntity {
    private String clusterName;
    private int supervisors;
    private int topologies;
    private int taskTotal;
    private String stormVersion;
    private int slotsTotal;
    private int slotsUsed;
    private int slotsFree;


    public ClusterEntity(String name, int supervisor_num, int topology_num,
                         String version, int total_ports, int used_ports, int task_num) {
        this.clusterName = name;
        this.supervisors = supervisor_num;
        this.topologies = topology_num;
        this.stormVersion = version;
        this.taskTotal = task_num;
        this.slotsTotal = total_ports;
        this.slotsUsed = used_ports;
        this.slotsFree = total_ports - used_ports;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getSupervisors() {
        return supervisors;
    }

    public void setSupervisors(int supervisors) {
        this.supervisors = supervisors;
    }

    public int getTopologies() {
        return topologies;
    }

    public void setTopologies(int topologies) {
        this.topologies = topologies;
    }

    public int getTaskTotal() {
        return taskTotal;
    }

    public void setTaskTotal(int taskTotal) {
        this.taskTotal = taskTotal;
    }

    public String getStormVersion() {
        return stormVersion;
    }

    public void setStormVersion(String stormVersion) {
        this.stormVersion = stormVersion;
    }

    public int getSlotsTotal() {
        return slotsTotal;
    }

    public void setSlotsTotal(int slotsTotal) {
        this.slotsTotal = slotsTotal;
    }

    public int getSlotsUsed() {
        return slotsUsed;
    }

    public void setSlotsUsed(int slotsUsed) {
        this.slotsUsed = slotsUsed;
    }

    public int getSlotsFree() {
        return slotsFree;
    }

    public void setSlotsFree(int slotsFree) {
        this.slotsFree = slotsFree;
    }
}
