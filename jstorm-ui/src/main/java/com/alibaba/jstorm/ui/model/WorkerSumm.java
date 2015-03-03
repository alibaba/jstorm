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
import java.util.List;

import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.WorkerSummary;

import com.alibaba.jstorm.common.stats.StatBuckets;

/**
 * mainpage:SupervisorSummary
 * 
 * @author longda
 * 
 */
public class WorkerSumm implements Serializable {

	private static final long serialVersionUID = -5631649054937247856L;
	private String port;
	private String topology;
	private String uptime;
	private String tasks;
	private String components;
	private String cpuNum;
	private String memNum;
	private String disks;
	private List<TaskSummary> taskSummList;

	public WorkerSumm() {
	}

	public WorkerSumm(WorkerSummary workerSummary) {
		this.port = String.valueOf(workerSummary.get_port());
		this.topology = workerSummary.get_topology();

		StringBuilder taskSB = new StringBuilder();
		StringBuilder componentSB = new StringBuilder();
		boolean isFirst = true;

		int minUptime = 0;
		taskSummList = workerSummary.get_tasks();
		for (TaskSummary taskSummary : taskSummList) {
			if (isFirst == false) {
				taskSB.append(',');
				componentSB.append(',');
			} else {
				minUptime = taskSummary.get_uptime_secs();
			}

			taskSB.append(taskSummary.get_task_id());
			componentSB.append(taskSummary.get_component_id());

			if (minUptime < taskSummary.get_uptime_secs()) {
				minUptime = taskSummary.get_uptime_secs();
			}

			isFirst = false;
		}

		this.uptime = StatBuckets.prettyUptimeStr(minUptime);
		this.tasks = taskSB.toString();
		this.components = componentSB.toString();

	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getTopology() {
		return topology;
	}

	public void setTopology(String topology) {
		this.topology = topology;
	}

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
	}

	public String getTasks() {
		return tasks;
	}

	public void setTasks(String tasks) {
		this.tasks = tasks;
	}

	public String getComponents() {
		return components;
	}

	public void setComponents(String components) {
		this.components = components;
	}

	public List<TaskSummary> gettaskSummList() {
		return taskSummList;
	}
	
	public void settaskSummList(List<TaskSummary> taskSummList) {
		this.taskSummList = taskSummList;
	}
}
