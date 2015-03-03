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
 * topologypage:Spouts/Bolts
 * 
 * @author xin.zhou
 * 
 */
import java.io.Serializable;

public class Components extends ComponentStats implements Serializable {

	private static final long serialVersionUID = -5697993689701474L;

	private String type; // spout/bolt
	private String componetId;
	private String parallelism;
	private String lastError;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getComponetId() {
		return componetId;
	}

	public void setComponetId(String componetId) {
		this.componetId = componetId;
	}

	public String getParallelism() {
		return parallelism;
	}

	public void setParallelism(String parallelism) {
		this.parallelism = parallelism;
	}

	public String getLastError() {
		return lastError;
	}

	public void setLastError(String lastError) {
		this.lastError = lastError;
	}

}
