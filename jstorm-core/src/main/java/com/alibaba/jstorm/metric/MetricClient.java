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
package com.alibaba.jstorm.metric;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.common.metric.*;
import com.codahale.metrics.Gauge;

/**
 * metric client for end users to add custom metrics.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
@SuppressWarnings("unused")
public class MetricClient {
    private static final String GROUP_UDF = "udf";

    private final String topologyId;
    private final String componentId;
    private final int taskId;

    public MetricClient(TopologyContext context) {
        taskId = context.getThisTaskId();
        this.topologyId = context.getTopologyId();
        this.componentId = context.getThisComponentId();
    }

    public AsmGauge registerGauge(String name, Gauge<Double> gauge) {
        return registerGauge(name, GROUP_UDF, gauge);
    }

    public AsmGauge registerGauge(String name, String group, Gauge<Double> gauge) {
        String userMetricName = getMetricName(name, group, MetricType.GAUGE);
        AsmGauge asmGauge = new AsmGauge(gauge);
        return (AsmGauge) JStormMetrics.registerTaskMetric(userMetricName, asmGauge);
    }

    public AsmCounter registerCounter(String name) {
        return registerCounter(name, GROUP_UDF);
    }

    public AsmCounter registerCounter(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.COUNTER);
        AsmCounter counter = new AsmCounter();
        return (AsmCounter) JStormMetrics.registerTaskMetric(userMetricName, counter);
    }

    public AsmMeter registerMeter(String name) {
        return registerMeter(name, GROUP_UDF);
    }

    public AsmMeter registerMeter(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.METER);
        return (AsmMeter) JStormMetrics.registerTaskMetric(userMetricName, new AsmMeter());
    }

    public AsmHistogram registerHistogram(String name) {
        return registerHistogram(name, GROUP_UDF);
    }

    public AsmHistogram registerHistogram(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.HISTOGRAM);
        return (AsmHistogram) JStormMetrics.registerTaskMetric(userMetricName, new AsmHistogram());
    }

    public AsmCounter registerTopologyCounter(String name) {
        String userMetricName = getMetricName(name, GROUP_UDF, MetricType.COUNTER);
        AsmCounter counter = new AsmCounter();
        return (AsmCounter) JStormMetrics.registerTaskTopologyMetric(userMetricName, counter);
    }

    public AsmMeter registerTopologyMeter(String name) {
        String userMetricName = getMetricName(name, GROUP_UDF, MetricType.METER);
        AsmMeter meter = new AsmMeter();
        return (AsmMeter) JStormMetrics.registerTaskTopologyMetric(userMetricName, meter);
    }

    public AsmHistogram registerTopologyHistogram(String name) {
        String userMetricName = getMetricName(name, GROUP_UDF, MetricType.HISTOGRAM);
        AsmHistogram histogram = new AsmHistogram();
        return (AsmHistogram) JStormMetrics.registerTaskTopologyMetric(userMetricName, histogram);
    }

    public void unregister(String name, MetricType type) {
        unregister(name, GROUP_UDF, type);
    }

    public void unregister(String name, String group, MetricType type) {
        String userMetricName = getMetricName(name, group, type);
        JStormMetrics.unregisterTaskMetric(userMetricName);
    }

    private String getMetricName(String name, MetricType type) {
        return getMetricName(name, GROUP_UDF, type);
    }

    private String getMetricName(String name, String group, MetricType type) {
        return MetricUtils.taskMetricName(topologyId, componentId, taskId, group, name, type);
    }
}
