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

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.KillTopologyEvent;

/**
 * The action when nimbus receives a killed command.
 * 1. change current topology status to killed
 * 2. wait for timeout seconds, do remove action, which removes topology from ZK
 *
 * @author Longda
 */
public class KillTransitionCallback extends DelayStatusTransitionCallback {

    public KillTransitionCallback(NimbusData data, String topologyId) {
        super(data, topologyId, null, StatusType.killed, StatusType.remove);
    }

    @Override
    public <T> Object execute(T... args) {
        KillTopologyEvent.pushEvent(topologyId);
        return super.execute(args);
    }

}
