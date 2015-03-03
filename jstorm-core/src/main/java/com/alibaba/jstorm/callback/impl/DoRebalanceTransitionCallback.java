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
package com.alibaba.jstorm.callback.impl;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyAssign;
import com.alibaba.jstorm.daemon.nimbus.TopologyAssignEvent;

/**
 * Do real rebalance action.
 * 
 * After nimbus receive one rebalance command, it will do as following: 1. set
 * topology status as rebalancing 2. delay 2 * timeout seconds 3. do this
 * callback
 * 
 * @author Xin.Li/Longda
 * 
 */
public class DoRebalanceTransitionCallback extends BaseCallback {

	private static Logger LOG = Logger
			.getLogger(DoRebalanceTransitionCallback.class);

	private String topologyid;
	private StormStatus oldStatus;

	public DoRebalanceTransitionCallback(NimbusData data, String topologyid,
			StormStatus status) {
		// this.data = data;
		this.topologyid = topologyid;
		this.oldStatus = status;
	}

	@Override
	public <T> Object execute(T... args) {
		try {
			TopologyAssignEvent event = new TopologyAssignEvent();

			event.setTopologyId(topologyid);
			event.setScratch(true);
			event.setOldStatus(oldStatus);

			TopologyAssign.push(event);

		} catch (Exception e) {
			LOG.error("do-rebalance error!", e);
		}
		// FIXME Why oldStatus?
		return oldStatus.getOldStatus();
	}

}
