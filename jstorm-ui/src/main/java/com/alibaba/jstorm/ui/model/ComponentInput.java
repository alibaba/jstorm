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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import backtype.storm.generated.GlobalStreamId;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * @author xin.zhou/Longda
 */
public class ComponentInput implements Serializable {

	private static final long serialVersionUID = -1415620236852453926L;

	private String componentId;
	private String stream;
	private String recvTps;
	private String process;
	private String acked;
	private String failed;

	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public String getRecvTps() {
		return recvTps;
	}

	public void setRecvTps(String recvTps) {
		this.recvTps = recvTps;
	}

	public String getProcess() {
		return process;
	}

	public void setProcess(String process) {
		this.process = process;
	}

	public String getAcked() {
		return acked;
	}

	public void setAcked(String acked) {
		this.acked = acked;
	}

	public String getFailed() {
		return failed;
	}

	public void setFailed(String failed) {
		this.failed = failed;
	}

	public void setValues(GlobalStreamId gstreamId, Double recvTps,
			Double process, Long acked, Long failed) {
		this.componentId = gstreamId.get_componentId();
		this.stream = gstreamId.get_streamId();
		this.recvTps = JStormUtils.formatValue(recvTps);
		this.acked = JStormUtils.formatValue(acked);
		this.failed = JStormUtils.formatValue(failed);
		this.process = JStormUtils.formatValue(process);

	}

}
