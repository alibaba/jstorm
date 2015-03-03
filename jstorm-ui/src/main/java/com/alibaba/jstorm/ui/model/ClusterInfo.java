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

import java.util.List;
import java.io.Serializable;

public class ClusterInfo implements Serializable {
	private static final long serialVersionUID = -7966384220162644896L;
	
	private String clusterName;
	private String zkRoot;
	private Integer zkPort;
	private List<String> zkServers;

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String name) {
		this.clusterName = name;
	}

	public String getZkRoot() {
		return zkRoot;
	}

	public void setZkRoot(String zkRoot) {
		this.zkRoot = zkRoot;
	}
	
	public Integer getZkPort() {
		return zkPort;
	}

	public void setZkPort(Integer zkPort) {
		this.zkPort = zkPort;
	}
	
	public List<String> getZkServers() {
		return zkServers;
	}

	public void setZkServers(List<String> zkServers) {
		this.zkServers = zkServers;
	}
}
