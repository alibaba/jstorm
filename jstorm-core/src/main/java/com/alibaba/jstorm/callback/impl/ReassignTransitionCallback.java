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

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyAssign;
import com.alibaba.jstorm.daemon.nimbus.TopologyAssignEvent;

/**
 * 1. call MonitorRunnable every Config.NIMBUS_MONITOR_FREQ_SECS
 * 2. MonitorRunnable will call NimbusData.transition
 * 3. NimbusData.transition will then call this callback
 */
public class ReassignTransitionCallback extends BaseCallback {

    private NimbusData data;
    private String topologyId;
    private StormStatus oldStatus;

    public ReassignTransitionCallback(NimbusData data, String topologyId) {
        this.data = data;
        this.topologyId = topologyId;
        this.oldStatus = null;
    }

    public ReassignTransitionCallback(NimbusData data, String topologyId, StormStatus oldStatus) {
        this.data = data;
        this.topologyId = topologyId;
        this.oldStatus = oldStatus;
    }

    @Override
    public <T> Object execute(T... args) {
        // default is true
        TopologyAssignEvent assignEvent = new TopologyAssignEvent();
        assignEvent.setTopologyId(topologyId);
        assignEvent.setScratch(false);
        assignEvent.setOldStatus(oldStatus);

        TopologyAssign.push(assignEvent);
        assignEvent.waitFinish();

        return null;
    }

}
