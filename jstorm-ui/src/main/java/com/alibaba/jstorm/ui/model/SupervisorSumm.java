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

import backtype.storm.generated.SupervisorSummary;

import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * mainpage:SupervisorSummary
 * 
 * @author xin.zhou
 * 
 */
public class SupervisorSumm implements Serializable {

	private static final long serialVersionUID = -5631649054937247850L;
	private String ip;

	private String host;
	private String uptime;
	private String totalPort;
	private String usedPort;

	public SupervisorSumm() {
	}

	public SupervisorSumm(SupervisorSummary s) {
		this.host = NetWorkUtils.ip2Host(s.get_host());
		this.ip = NetWorkUtils.host2Ip(s.get_host());
		this.uptime = StatBuckets.prettyUptimeStr(s.get_uptime_secs());

		this.totalPort = String.valueOf(s.get_num_workers());
		this.usedPort = String.valueOf(s.get_num_used_workers());
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getUptime() {
		return uptime;
	}

	public void setUptime(String uptime) {
		this.uptime = uptime;
	}

	public String getTotalPort() {
		return totalPort;
	}

	public void setTotalPort(String totalPort) {
		this.totalPort = totalPort;
	}

	public String getUsedPort() {
		return usedPort;
	}

	public void setUsedPort(String usedPort) {
		this.usedPort = usedPort;
	}

}
