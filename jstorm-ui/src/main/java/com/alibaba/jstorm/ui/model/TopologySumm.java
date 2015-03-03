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

import java.io.Serializable;

/**
 * mainpage:TopologySummary
 * 
 * @author xin.zhou
 * 
 */
public class TopologySumm implements Serializable {

	private static final long serialVersionUID = 189495975527682322L;
	private String topologyName;
	private String topologyId;
	private String status;
	private String uptime;
	private String numWorkers;
	private String numTasks;
	private String errorInfo;

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	public String getTopologyId() {
		return topologyId;
	}

	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
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

	public String getNumWorkers() {
		return numWorkers;
	}

	public void setNumWorkers(String numWorkers) {
		this.numWorkers = numWorkers;
	}

	public String getNumTasks() {
		return numTasks;
	}

	public void setNumTasks(String numTasks) {
		this.numTasks = numTasks;
	}
	
	public String getErrorInfo() {
		return this.errorInfo;
	}
	
	public void setErrorInfo(String errorInfo) {
		this.errorInfo = errorInfo;
	}

}
