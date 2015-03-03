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
 * mainpage:ClusterSummary
 * @author xin.zhou/zhiyuan.ls
 *
 */
import java.io.Serializable;

public class ClusterSumm implements Serializable {
	private static final long serialVersionUID = -7936384220562644886L;
	
	private String nimbusIp;
	private String nimbusLogPort;
	private String nimbusHostname;
	private String nimbusUptime;
	private String supervisorNum;
	private String runningTaskNum;
	private String totalPortSlotNum;
	private String usedPortSlotNum;
	private String freePortSlotNum;
	private String version;

	public String getNimbusIp() {
		return nimbusIp;
	}

	public void setNimbusIp(String nimbusIp) {
		this.nimbusIp = nimbusIp;
	}
	
	public String getNimbusLogPort() {
		return nimbusLogPort;
	}

	public void setNimbusLogPort(String nimbusLogPort) {
		this.nimbusLogPort = nimbusLogPort;
	}

	public String getNimbusHostname() {
		return nimbusHostname;
	}

	public void setNimbusHostname(String nimbusHostname) {
		this.nimbusHostname = nimbusHostname;
	}

	public String getNimbusUptime() {
		return nimbusUptime;
	}

	public void setNimbusUptime(String nimbusUptime) {
		this.nimbusUptime = nimbusUptime;
	}

	public String getSupervisorNum() {
		return supervisorNum;
	}

	public void setSupervisorNum(String supervisorNum) {
		this.supervisorNum = supervisorNum;
	}

	public String getRunningTaskNum() {
		return runningTaskNum;
	}

	public void setRunningTaskNum(String runningTaskNum) {
		this.runningTaskNum = runningTaskNum;
	}

	public String getTotalPortSlotNum() {
		return totalPortSlotNum;
	}

	public void setTotalPortSlotNum(String totalPortSlotNum) {
		this.totalPortSlotNum = totalPortSlotNum;
	}

	public String getUsedPortSlotNum() {
		return usedPortSlotNum;
	}

	public void setUsedPortSlotNum(String usedPortSlotNum) {
		this.usedPortSlotNum = usedPortSlotNum;
	}

	public String getFreePortSlotNum() {
		return freePortSlotNum;
	}

	public void setFreePortSlotNum(String freePortSlotNum) {
		this.freePortSlotNum = freePortSlotNum;
	}
	
	public String getVersion() {
		return version;
	}
	
	public void setVersion(String version) {
		this.version = version;
	}

}
