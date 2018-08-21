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

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.metric.TopologyMetricContext;

public class StartTopologyEvent extends MetricEvent {
    private double sampleRate;
    
    @Override
    public void run() {
        context.getMetricCache().putSampleRate(topologyId, sampleRate);
        context.getMetricUploaderDelegate().sendEvent(context.getClusterName(), this);
        
        if (!context.getTopologyMetricContexts().containsKey(topologyId)) {
            TopologyMetricContext metricContext = new TopologyMetricContext();
            // note that workerNum is not set here.
            context.getTopologyMetricContexts().put(topologyId, metricContext);
        }
    }
    
    public double getSampleRate() {
        return sampleRate;
    }
    
    public void setSampleRate(double sampleRate) {
        this.sampleRate = sampleRate;
    }
    
    public static void pushEvent(String topologyId, double sampleRate) {
        StartTopologyEvent startEvent = new StartTopologyEvent();
        startEvent.topologyId = topologyId;
        startEvent.sampleRate = sampleRate;
        ClusterMetricsRunnable.pushEvent(startEvent);
    }
    
}
