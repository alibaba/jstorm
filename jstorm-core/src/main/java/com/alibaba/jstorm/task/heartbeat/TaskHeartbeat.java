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
package com.alibaba.jstorm.task.heartbeat;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.stats.CommonStatsData;

/**
 * Task heartbeat, this Object will be updated to ZK timely
 * 
 * @author yannian
 * 
 */
public class TaskHeartbeat implements Serializable {

	private static final long serialVersionUID = -6369195955255963810L;
	private Integer timeSecs;
	private Integer uptimeSecs;
	private CommonStatsData stats; // BoltTaskStats or
									// SpoutTaskStats
	private String componentType;

	public TaskHeartbeat(int timeSecs, int uptimeSecs, CommonStatsData stats, String componentType) {
		this.timeSecs = timeSecs;
		this.uptimeSecs = uptimeSecs;
		this.stats = stats;
		this.componentType = componentType;
	}

	public int getTimeSecs() {
		return timeSecs;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
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

	public CommonStatsData getStats() {
		return stats;
	}

	public void setStats(CommonStatsData stats) {
		this.stats = stats;
	}
	
	public void setComponentType(String componentType) {
		this.componentType = componentType;
	}
	
	public String getComponentType() {
		return componentType;
	}

	@Override
	public boolean equals(Object hb) {
		if (hb instanceof TaskHeartbeat
				&& ((TaskHeartbeat) hb).timeSecs.equals(timeSecs)
				&& ((TaskHeartbeat) hb).uptimeSecs.equals(uptimeSecs)
				&& ((TaskHeartbeat) hb).stats.equals(stats)) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return timeSecs.hashCode() + uptimeSecs.hashCode() + stats.hashCode();
	}
}
