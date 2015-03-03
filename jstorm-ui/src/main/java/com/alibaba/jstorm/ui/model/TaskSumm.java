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
 * taskpage:TaskSummary
 * 
 * @author xin.zhou
 * 
 */
public class TaskSumm {

	private String taskid;
	private String host;
	private String port;
	private String topologyname;
	private String componentId;
	private String uptime;

	public TaskSumm(String taskid, String host, String port,
			String topologyname, String componentId, String uptime) {
		this.taskid = taskid;
		this.host = host;
		this.port = port;
		this.topologyname = topologyname;
		this.componentId = componentId;
		this.uptime = uptime;

	}

	public TaskSumm(String taskid, String host, String port, String uptime) {
		this.taskid = taskid;
		this.host = host;
		this.port = port;
		this.uptime = uptime;

	}

	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getTopologyname() {
		return topologyname;
	}

	public void setTopologyname(String topologyname) {
		this.topologyname = topologyname;
	}

	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
	}
}
