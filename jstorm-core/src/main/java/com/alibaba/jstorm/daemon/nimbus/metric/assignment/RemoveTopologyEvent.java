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
package com.alibaba.jstorm.daemon.nimbus.metric.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;

public class RemoveTopologyEvent extends MetricEvent {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveTopologyEvent.class);
    
    public void removeTopology(String topologyId) {
        context.getMetricCache().removeTopology(topologyId);
        context.getMetricCache().removeSampleRate(topologyId);
        context.getTopologyMetricContexts().remove(topologyId);
    }
    
    @Override
    public void run() {
        if (topologyId != null) {
            removeTopology(topologyId);
        }
        LOG.info("remove topology:{}.", topologyId);
    }
    
    public static void pushEvent(String topologyId) {
        RemoveTopologyEvent event = new RemoveTopologyEvent();
        event.setTopologyId(topologyId);
        event.setClusterMetricsContext(ClusterMetricsRunnable.getInstance().getContext());
        
//        ClusterMetricsRunnable.pushEvent(event);
        
        // directly remove event, skip issue remove event
        event.run();
    }
}
