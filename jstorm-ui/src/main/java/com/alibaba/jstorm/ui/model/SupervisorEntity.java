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

import backtype.storm.generated.SupervisorSummary;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class SupervisorEntity {
    private String id;
    private String host;
    private String ip;
    private String uptime;
    private int uptimeSeconds;
    private int slotsTotal;
    private int slotsUsed;

    public SupervisorEntity(SupervisorSummary supervisor){
        this(supervisor.get_supervisorId(), supervisor.get_host(), supervisor.get_uptimeSecs(),
                supervisor.get_numWorkers(), supervisor.get_numUsedWorkers());
    }

    public SupervisorEntity(String id, String host, int uptimeSeconds, int slotsTotal, int slotsUsed) {
        this.id = id;
        this.host = NetWorkUtils.ip2Host(host);
        this.ip = NetWorkUtils.host2Ip(host);
        this.uptimeSeconds = uptimeSeconds;
        this.uptime = UIUtils.prettyUptime(JStormUtils.parseInt(uptimeSeconds, 0));
        this.slotsTotal = slotsTotal;
        this.slotsUsed = slotsUsed;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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
}
