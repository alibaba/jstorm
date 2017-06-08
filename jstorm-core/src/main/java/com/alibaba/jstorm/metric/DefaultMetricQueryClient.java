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

import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.common.metric.TaskTrack;
import com.alibaba.jstorm.common.metric.TopologyHistory;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * a dummy metric query client implementation
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class DefaultMetricQueryClient implements MetricQueryClient {
    @Override
    public void init(Map conf) {
    }

    @Override
    public boolean isInited() {
        return true;
    }

    @Override
    public String getIdentity(Map conf) {
        return getClass().getCanonicalName();
    }

    @Override
    public List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type, MetaFilter filter, Object arg) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getWorkerMeta(String clusterName, String topologyId) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getNettyMeta(String clusterName, String topologyId) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getTaskMeta(String clusterName, String topologyId, int taskId) {
        return Lists.newArrayList();
    }

    @Override
    public List<MetricMeta> getComponentMeta(String clusterName, String topologyId, String componentId) {
        return Lists.newArrayList();
    }

    @Override
    public MetricMeta getMetricMeta(String clusterName, String topologyId, MetaType metaType, String metricId) {
        return new MetricMeta();
    }

    @Override
    public MetricMeta getMetricMeta(String key) {
        return new MetricMeta();
    }

    @Override
    public List<Object> getMetricData(String metricId, MetricType metricType, int win, long start, long end) {
        return Lists.newArrayList();
    }

    @Override
    public List<Object> getMetricData(String metricId, MetricType metricType, int win, long start, long end, int size) {
        return Lists.newArrayList();
    }

    @Override
    public List<TaskTrack> getTaskTrack(String clusterName, String topologyId) {
        return Lists.newArrayList();
    }

    @Override
    public List<TaskTrack> getTaskTrack(String clusterName, String topologyId, int taskId) {
        return Lists.newArrayList();
    }

    @Override
    public List<TopologyHistory> getTopologyHistory(String clusterName, String topologyName, int size) {
        return Lists.newArrayList();
    }

    @Override
    public void deleteMeta(MetricMeta meta) {
    }

    @Override
    public void deleteMeta(List<MetricMeta> metaList) {
    }
}
