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
 * componentpage:ComponentOutput
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;

import com.alibaba.jstorm.utils.JStormUtils;

public class ComponentOutput implements Serializable {

	private static final long serialVersionUID = 5607257248459397567L;

	private String stream;
	private String emitted;
	private String sendTps;

	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public String getEmitted() {
		return emitted;
	}

	public void setEmitted(String emitted) {
		this.emitted = emitted;
	}

	public String getSendTps() {
		return sendTps;
	}

	public void setSendTps(String sendTps) {
		this.sendTps = sendTps;
	}

	public void setValues(String stream, Long emitted, Double sendTps) {
		this.stream = stream;
		this.emitted = JStormUtils.formatValue(emitted);
		this.sendTps = JStormUtils.formatValue(sendTps);
	}

}