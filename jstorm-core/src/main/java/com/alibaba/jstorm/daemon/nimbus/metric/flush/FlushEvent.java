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
package com.alibaba.jstorm.daemon.nimbus.metric.flush;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.metric.TopologyMetricContext;

public class FlushEvent extends MetricEvent {
    private static final Logger LOG = LoggerFactory.getLogger(FlushEvent.class);

    @Override
    public void run() {
        if (!context.getNimbusData().isLeader()) {
            return;
        }

        // if metricUploader is not fully initialized, return directly
        if (!context.isReadyToUpload()) {
            LOG.info("Context Metric Uploader isn't ready");
            return;
        }

        try {
            long start = System.currentTimeMillis();
            for (Map.Entry<String, TopologyMetricContext> entry : context.getTopologyMetricContexts().entrySet()) {
                String topologyId = entry.getKey();
                TopologyMetricContext metricContext = entry.getValue();

                Map<String, Long> cachedMeta = context.getMetricCache().getMeta(topologyId);
                if (cachedMeta == null) {
                    cachedMeta = new HashMap<>();
                }
                Map<String, Long> memMeta = metricContext.getMemMeta();
                if (memMeta.size() > cachedMeta.size()) {
                    cachedMeta.putAll(memMeta);
                }
                context.getMetricCache().putMeta(topologyId, cachedMeta);
                metricContext.setSyncMeta(false);

                int curSize = cachedMeta.size();
                if (curSize != metricContext.getFlushedMetaNum()) {
                    metricContext.setFlushedMetaNum(curSize);

                    context.getMetricUploaderDelegate().registerMetrics(context.getClusterName(), topologyId, cachedMeta);
                    LOG.info("Flush metric meta, topology:{}, total:{}, cost:{}.", topologyId, curSize,
                            System.currentTimeMillis() - start);
                }
                context.getStormClusterState().set_topology_metric(topologyId, curSize);
            }

        } catch (Exception ex) {
            LOG.error("Error", ex);
        }
    }
}
