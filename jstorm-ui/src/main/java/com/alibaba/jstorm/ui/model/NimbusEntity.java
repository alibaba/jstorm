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

import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class NimbusEntity {
    private String host;
    private String ip;
    private String port;
    private String status;
    private String nimbusUpTime;
    private String nimbusUpTimeSeconds;
    private String version;


    public NimbusEntity(String host, String uptime_secs, String version) {
        this.ip= UIUtils.getIp(host);
        this.port = UIUtils.getPort(host);
        this.host = NetWorkUtils.ip2Host(ip);
        this.nimbusUpTime = UIUtils.prettyUptime(JStormUtils.parseInt(uptime_secs, 0));
        this.nimbusUpTimeSeconds = uptime_secs;
        this.version = version;
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

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getNimbusUpTime() {
        return nimbusUpTime;
    }

    public void setNimbusUpTime(String nimbusUpTime) {
        this.nimbusUpTime = nimbusUpTime;
    }

    public String getNimbusUpTimeSeconds() {
        return nimbusUpTimeSeconds;
    }

    public void setNimbusUpTimeSeconds(String nimbusUpTimeSeconds) {
        this.nimbusUpTimeSeconds = nimbusUpTimeSeconds;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
