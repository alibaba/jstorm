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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.schedule.DelayEventRunnable;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * 
 * The action when nimbus receive kill command 1. set the topology status as target 2. wait 2 * Timeout seconds later, do removing topology from ZK
 * 
 * @author Longda
 */
public class DelayStatusTransitionCallback extends BaseCallback {

    private static Logger LOG = LoggerFactory.getLogger(DelayStatusTransitionCallback.class);

    public static final int DEFAULT_DELAY_SECONDS = 30;

    protected NimbusData data;
    protected String topologyid;
    protected StormStatus oldStatus;
    protected StatusType newType;
    protected StatusType nextAction;

    public DelayStatusTransitionCallback(NimbusData data, String topologyid, StormStatus oldStatus, StatusType newType, StatusType nextAction) {
        this.data = data;
        this.topologyid = topologyid;
        this.oldStatus = oldStatus;
        this.newType = newType;
        this.nextAction = nextAction;
    }

    public int getDelaySeconds(Object[] args) {
        if (oldStatus != null && oldStatus.getDelaySecs() > 0) {
            return oldStatus.getDelaySecs();
        }

        Integer delaySecs = DelayStatusTransitionCallback.DEFAULT_DELAY_SECONDS;
        if (args == null || args.length == 0 || args[0] == null) {
            Map<?, ?> map = null;
            try {

                map = StormConfig.read_nimbus_topology_conf(topologyid, data.getBlobStore());
                delaySecs = JStormUtils.parseInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), DEFAULT_DELAY_SECONDS);
            } catch (Exception e) {
                LOG.info("Failed to get topology configuration " + topologyid);
            }

        } else {
            delaySecs = JStormUtils.parseInt(args[0]);
        }

        if (delaySecs == null || delaySecs < 0) {
            delaySecs = DelayStatusTransitionCallback.DEFAULT_DELAY_SECONDS;
        }

        return delaySecs;
    }

    @Override
    public <T> Object execute(T... args) {
        int delaySecs = getDelaySeconds(args);
        LOG.info("Delaying event " + newType + " for " + delaySecs + " secs for " + topologyid);

        data.getScheduExec().schedule(new DelayEventRunnable(data, topologyid, nextAction, args), delaySecs, TimeUnit.SECONDS);

        return new StormStatus(delaySecs, newType);
    }

}
