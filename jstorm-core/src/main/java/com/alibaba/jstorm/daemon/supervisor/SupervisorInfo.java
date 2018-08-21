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

import com.alibaba.jstorm.client.ConfigExtension;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Object stored in ZK /ZK-DIR/supervisors
 *
 * @author Xin.Zhou/Longda
 */
public class SupervisorInfo implements Serializable {

    private static final long serialVersionUID = -8384417078907518922L;

    private final String hostName;
    private final String supervisorId;

    private Integer timeSecs;
    private Integer uptimeSecs;
    private String version;
    private String buildTs;
    private Integer port;

    private Set<Integer> workerPorts;
    private Map<Object, Object> supervisorConf;

    private String errorMessage;

    private transient Set<Integer> availableWorkerPorts;

    public SupervisorInfo(String hostName, String supervisorId, Set<Integer> workerPorts, Map<Object, Object> conf) {
        this.hostName = hostName;
        this.supervisorId = supervisorId;
        this.workerPorts = workerPorts;
        this.supervisorConf = conf;
        this.port = ConfigExtension.getSupervisorDeamonHttpserverPort(conf);
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getHostName() {
        return hostName;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public int getTimeSecs() {
        return timeSecs;
    }

    public void setTimeSecs(int timeSecs) {
        this.timeSecs = timeSecs;
    }

    public int getUptimeSecs() {
        return uptimeSecs;
    }

    public void setUptimeSecs(int uptimeSecs) {
        this.uptimeSecs = uptimeSecs;
    }

    public Set<Integer> getWorkerPorts() {
        return workerPorts;
    }

    public void setAvailableWorkerPorts(Set<Integer> workerPorts) {
        if (availableWorkerPorts == null)
            availableWorkerPorts = new HashSet<>();
        availableWorkerPorts.addAll(workerPorts);
    }

    public Set<Integer> getAvailableWorkerPorts() {
        if (availableWorkerPorts == null)
            availableWorkerPorts = new HashSet<>();
        return availableWorkerPorts;
    }

    public void setWorkerPorts(Set<Integer> workerPorts) {
        this.workerPorts = workerPorts;
    }

    public Map<Object, Object> getSupervisorConf() {
        return supervisorConf;
    }

    public SupervisorInfo setSupervisorConf(Map<Object, Object> conf) {
        this.supervisorConf = conf;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBuildTs() {
        return buildTs;
    }

    public SupervisorInfo setBuildTs(String buildTs) {
        this.buildTs = buildTs;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public SupervisorInfo setPort(Integer port) {
        this.port = port;
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
        result = prime * result + ((supervisorId == null) ? 0 : supervisorId.hashCode());
        result = prime * result + ((timeSecs == null) ? 0 : timeSecs.hashCode());
        result = prime * result + ((uptimeSecs == null) ? 0 : uptimeSecs.hashCode());
        result = prime * result + ((workerPorts == null) ? 0 : workerPorts.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SupervisorInfo other = (SupervisorInfo) obj;
        if (hostName == null) {
            if (other.hostName != null)
                return false;
        } else if (!hostName.equals(other.hostName))
            return false;
        if (supervisorId == null) {
            if (other.supervisorId != null)
                return false;
        } else if (!supervisorId.equals(other.supervisorId))
            return false;
        if (timeSecs == null) {
            if (other.timeSecs != null)
                return false;
        } else if (!timeSecs.equals(other.timeSecs))
            return false;
        if (uptimeSecs == null) {
            if (other.uptimeSecs != null)
                return false;
        } else if (!uptimeSecs.equals(other.uptimeSecs))
            return false;
        if (workerPorts == null) {
            if (other.workerPorts != null)
                return false;
        } else if (!workerPorts.equals(other.workerPorts))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    /**
     * get Map<supervisorId, hostname>
     */
    public static Map<String, String> getNodeHost(Map<String, SupervisorInfo> supInfos) {
        Map<String, String> rtn = new HashMap<>();
        for (Entry<String, SupervisorInfo> entry : supInfos.entrySet()) {
            SupervisorInfo superInfo = entry.getValue();
            String supervisorId = entry.getKey();
            rtn.put(supervisorId, superInfo.getHostName());
        }
        return rtn;
    }

}