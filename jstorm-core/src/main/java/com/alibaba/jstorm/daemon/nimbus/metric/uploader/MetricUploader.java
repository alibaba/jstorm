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

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.config.Refreshable;
import com.alibaba.jstorm.config.RefreshableComponents;
import com.alibaba.jstorm.metric.MetaType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;

import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.Utils;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MetricUploader {
    /**
     * Set NimbusData to MetricUploader
     */
    void init(NimbusData nimbusData) throws Exception;

    void cleanup();

    /**
     * register metrics to external metric plugin
     */
    boolean registerMetrics(String clusterName, String topologyId, Map<String, Long> metrics) throws Exception;

    String METRIC_TYPE = "metric.type";
    String METRIC_TYPE_TOPLOGY = "TP";
    String METRIC_TYPE_TASK = "TASK";
    String METRIC_TYPE_ALL = "ALL";
    String METRIC_TIME = "metric.timestamp";
    String NIMBUS_CONF_PREFIX = "nimbus.metrics.plugin.";
    String INSERT_FILTER_SUFFIX = ".insert.filters";


    /**
     * upload topologyMetric to external metric plugin (such as database plugin)
     *
     * @return true means success, false means failure
     */
    @Deprecated
    boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric, Map<String, Object> metricContext);

    /**
     * upload metrics with given key and metric context. the implementation can
     * retrieve metric data from rocks db
     * in the handler thread, which is kind of lazy-init, making it more
     * GC-friendly
     */
    boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext);

    /**
     * Send an event to underlying handler
     */
    boolean sendEvent(String clusterName, MetricEvent event);

    /**
     * global rate controller, shared by all metric uploaders
     */
    UploadRateController rateController = new UploadRateController();

    class UploadRateController implements Refreshable {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private volatile int maxConcurrentUploadingNum;
        private final AtomicInteger currentUploadingNum = new AtomicInteger(0);
        private boolean enableRateControl;

        public UploadRateController() {
            RefreshableComponents.registerRefreshable(this);
        }

        public void init(Map conf) {
            this.maxConcurrentUploadingNum = ConfigExtension.getMaxConcurrentUploadingNum(conf);
            this.enableRateControl = ConfigExtension.isEnableMetricUploadRateControl(conf);
            logger.info("Enable rate control:{}, max concurrent uploading num:{}",
                    enableRateControl, maxConcurrentUploadingNum);
        }

        public boolean isEnableRateControl() {
            return enableRateControl;
        }

        public void incrUploadingNum() {
            int num = currentUploadingNum.incrementAndGet();
            logger.debug("incr, UploadingNum:{}", num);
        }

        public void decrUploadingNum() {
            int num = currentUploadingNum.decrementAndGet();
            logger.debug("decr, UploadingNum:{}", num);
        }

        public synchronized boolean syncToUpload() {
            if (currentUploadingNum.get() < maxConcurrentUploadingNum) {
                incrUploadingNum();
                return true;
            }
            return false;
        }

        @Override
        public void refresh(Map conf) {
            int maxUploadingNum = ConfigExtension.getMaxConcurrentUploadingNum(conf);
            if (maxUploadingNum > 0 && maxUploadingNum != this.maxConcurrentUploadingNum) {
                this.maxConcurrentUploadingNum = maxUploadingNum;
            }
        }
    }

    class MetricUploadFilter {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        protected final Set<MetaType> insertFilters = new HashSet<>();

        public void parseInsertFilters(Map conf, String key) {
            String insertMetaTypes = Utils.getString(conf.get(key), "NONE");
            if ("ALL".equals(insertMetaTypes)) {
                Collections.addAll(insertFilters, MetaType.values());
            } else if (!"NONE".equals(insertMetaTypes)) {
                String[] metaTypes = insertMetaTypes.split(",");
                for (String metaType : metaTypes) {
                    try {
                        MetaType m = MetaType.valueOf(MetaType.class, metaType.trim());
                        insertFilters.add(m);
                    } catch (Exception ignored) {
                        logger.warn("Bad meta type:{}", metaType);
                    }
                }
            }
        }

        public boolean filter(MetaType metaType) {
            return insertFilters.contains(metaType);
        }
    }

}
