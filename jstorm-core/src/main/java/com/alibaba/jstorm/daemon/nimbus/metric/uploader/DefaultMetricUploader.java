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
package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsContext;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;

import backtype.storm.generated.TopologyMetric;

public class DefaultMetricUploader implements MetricUploader {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected NimbusData nimbusData;
    protected ClusterMetricsContext context;

    public DefaultMetricUploader() {
    }

    @Override
    public void init(NimbusData nimbusData) throws Exception {
        this.nimbusData = nimbusData;
        this.context = ClusterMetricsRunnable.getInstance().getContext();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public boolean registerMetrics(String clusterName, String topologyId, Map<String, Long> metrics) {
        if (metrics.size() > 0) {
            logger.info("register metrics, topology:{}, total:{}", topologyId, metrics.size());
        }
        return true;
    }

    @Override
    public boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric,
                          Map<String, Object> metricContext) {
        if (tpMetric == null) {
            logger.info("No metric of {}", topologyId);
            return true;
        }

        int totalSize = tpMetric.get_topologyMetric().get_metrics_size()
                + tpMetric.get_componentMetric().get_metrics_size() + tpMetric.get_taskMetric().get_metrics_size()
                + tpMetric.get_streamMetric().get_metrics_size() + tpMetric.get_workerMetric().get_metrics_size()
                + tpMetric.get_nettyMetric().get_metrics_size();

        logger.info("send metrics, cluster:{}, topology:{}, metric size:{}, metricContext:{}", clusterName, topologyId,
                totalSize, metricContext);

        return true;
    }

    @Override
    public boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext) {
        context.markUploaded(this, (Integer) key);
        logger.info("upload metrics, cluster:{}, topology:{}, key:{}", clusterName, topologyId, key);
        return true;
    }

    @Override
    public boolean sendEvent(String clusterName, MetricEvent event) {
        logger.info("Successfully sendEvent {} of {}", event, clusterName);
        return true;
    }
}
