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
package com.alibaba.jstorm.task.master.metrics;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMaster;
import com.alibaba.jstorm.task.master.TopologyMasterContext;

import backtype.storm.generated.WorkerUploadMetrics;
import backtype.storm.tuple.Tuple;

/**
 * updates metric data for topology workers
 */
@SuppressWarnings("unused")
public class MetricsUpdater implements TMHandler {
    static private final Logger LOG = LoggerFactory.getLogger(MetricsUpdater.class);
    static private final Logger metricLogger = TopologyMetricContext.LOG;

    private TopologyMetricContext topologyMetricContext;
    private TopologyMasterContext tmContext;

    @Override
    public void init(TopologyMasterContext tmContext) {
        this.tmContext = tmContext;
        this.topologyMetricContext = tmContext.getTopologyMetricContext();
    }

    @Override
    public void process(Object event) {
        Tuple input = (Tuple) event;
        updateMetrics(input);
    }

    @Override
    public void cleanup() {
    }

    private void updateMetrics(Tuple input) {
        String workerSlot = (String) input.getValueByField(TopologyMaster.FIELD_METRIC_WORKER);
        WorkerUploadMetrics metrics = (WorkerUploadMetrics) input.getValueByField(TopologyMaster.FIELD_METRIC_METRICS);
        topologyMetricContext.addToMemCache(workerSlot, metrics.get_allMetrics());
        metricLogger.info("received metrics from:{}, size:{}", workerSlot, metrics.get_allMetrics().get_metrics_size());
    }

}
