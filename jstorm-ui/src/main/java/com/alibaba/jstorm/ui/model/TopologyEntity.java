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

import com.alibaba.jstorm.ui.utils.UIUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class TopologyEntity {
    private String id;
    private String name;
    private String status;
    private String uptime;
    private int uptimeSeconds;
    private int tasksTotal;
    private int workersTotal;
    private String errorInfo;

    public TopologyEntity(String id, String name, String status, int uptimeSeconds,
                          int tasksTotal, int workersTotal, String errorInfo) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.uptime = UIUtils.prettyUptime(uptimeSeconds);
        this.uptimeSeconds = uptimeSeconds;
        this.tasksTotal = tasksTotal;
        this.workersTotal = workersTotal;
        this.errorInfo = errorInfo;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    public int getTasksTotal() {
        return tasksTotal;
    }

    public void setTasksTotal(int tasksTotal) {
        this.tasksTotal = tasksTotal;
    }

    public int getWorkersTotal() {
        return workersTotal;
    }

    public void setWorkersTotal(int workersTotal) {
        this.workersTotal = workersTotal;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }
}
