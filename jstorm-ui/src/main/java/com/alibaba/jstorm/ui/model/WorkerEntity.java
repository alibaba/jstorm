/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.ui.model;

import backtype.storm.generated.TaskComponent;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.WorkerSummary;
import com.alibaba.jstorm.ui.utils.UIUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class WorkerEntity {
    private String topology;    //topology id
    private int port;
    private String uptime;
    private int uptimeSeconds;
    private List<Map<String, Object>> tasks;

    public WorkerEntity(WorkerSummary worker){
        this.topology = worker.get_topology();
        this.port = worker.get_port();
        this.uptimeSeconds = worker.get_uptime();
        this.uptime = UIUtils.prettyUptime(uptimeSeconds);
        tasks = new ArrayList<>();
        for (TaskComponent task : worker.get_tasks()){
            Map<String, Object> t = new HashMap<>();
            t.put("id", task.get_taskId());
            t.put("component", task.get_component());
            tasks.add(t);
        }
    }



    public String getTopology() {
        return topology;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public int getUptimeSeconds() {
        return uptimeSeconds;
    }

    public void setUptimeSeconds(int uptimeSeconds) {
        this.uptimeSeconds = uptimeSeconds;
    }

    public List<Map<String, Object>> getTasks() {
        return tasks;
    }

    public void setTasks(List<Map<String, Object>> tasks) {
        this.tasks = tasks;
    }
}
