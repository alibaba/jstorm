/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TaskBaseMetric implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(JStormMetrics.class);

    private static final long serialVersionUID = -7157987126460293444L;
    private String topologyId;
    private String componentId;
    private int taskId;

    /**
     * local metric name cache to avoid frequent metric name concatenation taskId + streamId + name ==> full metric name
     */
    private final ConcurrentMap<String, AsmMetric> metricCache = new ConcurrentHashMap<String, AsmMetric>();

    public TaskBaseMetric(String topologyId, String componentId, int taskId) {
        this.topologyId = topologyId;
        this.componentId = componentId;
        this.taskId = taskId;
        logger.info("init task base metric, tp id:{}, comp id:{}, task id:{}", topologyId, componentId, taskId);
    }

    private AsmMetric findMetric(String streamId, String name, MetricType metricType, boolean mergeTopology) {
        //String key = new StringBuilder(30).append(taskId).append(streamId).append(name).toString();
        String key = streamId + name;
        AsmMetric existingMetric = metricCache.get(key);
        if (existingMetric == null) {
            String fullName = MetricUtils.streamMetricName(topologyId, componentId, taskId, streamId, name, metricType);
            existingMetric = JStormMetrics.getStreamMetric(fullName);
            if (existingMetric == null) {
                existingMetric = AsmMetric.Builder.build(metricType);
                JStormMetrics.registerStreamMetric(fullName, existingMetric, mergeTopology);
            }
            AsmMetric oldMetric = metricCache.putIfAbsent(key, existingMetric);
            if (oldMetric != null) {
                existingMetric = oldMetric;
            }
        }

        return existingMetric;
    }

    public void update(final String streamId, final String name, final Number value,
                       final MetricType metricType, boolean mergeTopology) {
        AsmMetric existingMetric = findMetric(streamId, name, metricType, mergeTopology);
        existingMetric.update(value);
    }

    public void updateTime(String streamId, String name, long start, long end, boolean mergeTopology) {
        updateTime(streamId, name, start, end, 1, mergeTopology);
    }

    /**
     * almost the same implementation of above update, but improves performance for histograms
     */
    public void updateTime(String streamId, String name, long start, long end, int count, boolean mergeTopology) {
        if (start > 0) {
            AsmMetric existingMetric = findMetric(streamId, name, MetricType.HISTOGRAM, mergeTopology);

            if (existingMetric instanceof AsmHistogram) {
                AsmHistogram histogram = (AsmHistogram) existingMetric;
                if (histogram.okToUpdate(end)) {
                    long elapsed = ((end - start) * TimeUtils.US_PER_MS) / count;
                    if (elapsed >= 0) {
                        histogram.update(elapsed);
                        histogram.setLastUpdateTime(end);
                    }
                }
            }
        }
    }

    public void update(final String streamId, final String name, final Number value, final MetricType metricType) {
        update(streamId, name, value, metricType, true);
    }

    public void send_tuple(String stream, int num_out_tasks) {
        if (JStormMetrics.enabled && num_out_tasks > 0) {
            update(stream, MetricDef.EMMITTED_NUM, num_out_tasks, MetricType.COUNTER);
            update(stream, MetricDef.SEND_TPS, num_out_tasks, MetricType.METER);
        }
    }

    public void recv_tuple(String component, String stream) {
        recv_tuple(component, stream, 1);
    }

    public void recv_tuple(String component, String stream, int tupleNum) {
        if (JStormMetrics.enabled) {
            update(stream, fastConcat(component, MetricDef.RECV_TPS), tupleNum, MetricType.METER);
        }
    }

    public void tupleLifeCycle(String component, String stream, long lifeCycleStart, long endTime) {
        updateTime(stream, fastConcat(component, MetricDef.TUPLE_LIEF_CYCLE), lifeCycleStart, endTime, false);
    }

    public void bolt_acked_tuple(String component, String stream) {
        bolt_acked_tuple(component, stream, 1);
    }

    public void bolt_acked_tuple(String component, String stream, int tupleNum) {
        if (JStormMetrics.enabled) {
            update(stream, MetricDef.ACKED_NUM, tupleNum, MetricType.COUNTER);
        }
    }

    public void update_bolt_acked_latency(String component, String stream, long latencyStart, long endTime) {
        update_bolt_acked_latency(component, stream, latencyStart, endTime, 1);
    }

    public void update_bolt_acked_latency(String component, String stream, long latencyStart, long endTime, int tupleNum) {
        if (JStormMetrics.enabled) {
            updateTime(stream, MetricDef.PROCESS_LATENCY, latencyStart, endTime, tupleNum, false);
        }
    }


    @SuppressWarnings("unused")
    public void bolt_failed_tuple(String component, String stream) {
        if (JStormMetrics.enabled) {
            update(stream, MetricDef.FAILED_NUM, 1, MetricType.COUNTER);
        }
    }

    public void spout_acked_tuple(String stream, long latencyStart, long lifeCycleStart, long endTime) {
        if (JStormMetrics.enabled) {
            update(stream, MetricDef.ACKED_NUM, 1, MetricType.COUNTER);
            updateTime(stream, MetricDef.PROCESS_LATENCY, latencyStart, endTime, false);
            updateTime(stream, fastConcat(Common.ACKER_COMPONENT_ID, MetricDef.TUPLE_LIEF_CYCLE), lifeCycleStart, endTime, false);
        }
    }

    private String fastConcat(String componentId, String metricName) {
        StringBuilder sb = new StringBuilder(32);
        return sb.append(componentId).append(".").append(metricName).toString();
    }

    public void spout_failed_tuple(String stream) {
        if (JStormMetrics.enabled) {
            update(stream, MetricDef.FAILED_NUM, 1, MetricType.COUNTER);
        }
    }

    public static void main(String[] args) {
        TaskBaseMetric taskBaseMetric = new TaskBaseMetric("topo1", "spout", 1);
        taskBaseMetric.update("_topology_master", "RecvTps", 1, MetricType.METER, true);
    }
}
