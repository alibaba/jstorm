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
package com.alibaba.jstorm.daemon.nimbus.metric;

import com.alibaba.jstorm.common.metric.MetricMeta;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.metric.flush.FlushEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.merge.MergeEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.refresh.RefreshEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.DefaultMetricUploader;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.TopologyMetricDataInfo;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.UploadEvent;
import com.alibaba.jstorm.metric.DefaultMetricIDGenerator;
import com.alibaba.jstorm.metric.DefaultMetricQueryClient;
import com.alibaba.jstorm.metric.JStormMetricCache;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricIDGenerator;
import com.alibaba.jstorm.metric.MetricQueryClient;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.SimpleJStormMetric;
import com.alibaba.jstorm.metric.TimeTicker;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.codahale.metrics.Gauge;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.Utils;

public class ClusterMetricsContext {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterMetricsContext.class);

    protected JStormMetricCache metricCache;

    /**
     * map<topologyId, TopologyMetricContext>>, local memory cache, keeps only one snapshot of metrics.
     */
    protected final ConcurrentMap<String, TopologyMetricContext> topologyMetricContexts = new ConcurrentHashMap<>();

    public static final String PENDING_UPLOAD_METRIC_DATA = "__pending.upload.metrics__";
    public static final String PENDING_UPLOAD_METRIC_DATA_INFO = "__pending.upload.metrics.info__";

    // the slot is empty
    private static final int UNSET = 0;
    // the slot is ready for uploading
    private static final int SET = 1;
    // the slot is being uploaded
    private static final int UPLOADING = 2;
    // the slot will be set ready for uploading
    private static final int PRE_SET = 3;

    protected final AtomicIntegerArray metricStat;

    protected StormClusterState stormClusterState;

    private MetricUploaderDelegate metricUploaderDelegate;
    protected final List<MetricUploader> metricUploaders = new ArrayList<>();
    private MetricUploader lastMetricUploader;
    private final AtomicBoolean readyToUpload = new AtomicBoolean(false);

    protected AtomicBoolean isShutdown;
    protected String clusterName;
    protected int maxPendingUploadMetrics;

    private final NimbusData nimbusData;
    private MetricQueryClient metricQueryClient;

    /**
     * use default UUID generator
     */
    private final MetricIDGenerator metricIDGenerator = new DefaultMetricIDGenerator();

    public ClusterMetricsContext(final NimbusData nimbusData) {
        LOG.info("create cluster metrics context...");

        this.nimbusData = nimbusData;
        this.metricCache = nimbusData.getMetricCache();
        this.stormClusterState = nimbusData.getStormClusterState();
        this.isShutdown = nimbusData.getIsShutdown();
        clusterName = ConfigExtension.getClusterName(nimbusData.getConf());
        if (clusterName == null) {
            throw new RuntimeException("cluster.name property must be set in storm.yaml!");
        }

        this.maxPendingUploadMetrics = ConfigExtension.getMaxPendingMetricNum(nimbusData.getConf());
        this.metricStat = new AtomicIntegerArray(this.maxPendingUploadMetrics);

        int cnt = 0;
        for (int i = 0; i < maxPendingUploadMetrics; i++) {
            TopologyMetricDataInfo obj = getMetricDataInfoFromCache(i);
            if (obj != null) {
                this.metricStat.set(i, SET);
                cnt++;
            }
        }
        LOG.info("pending upload metrics: {}", cnt);

        // track nimbus JVM heap
        JStormMetrics.registerWorkerGauge(JStormMetrics.NIMBUS_METRIC_KEY, MetricDef.MEMORY_USED,
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getJVMHeapMemory();
                    }
                }));
    }

    /**
     * init plugins and start event
     */
    public void init() {
        try {
            initPlugin();
        } catch (RuntimeException e) {
            LOG.error("init metrics plugin error:", e);
            System.exit(-1);
        }

        pushRefreshEvent();
        pushFlushEvent();
        pushMergeEvent();
        pushUploadEvent();
        pushDiagnosisEvent();
        LOG.info("Finish");
    }

    public void initPlugin() {
        // init rate controller
        MetricUploader.rateController.init(nimbusData.getConf());

        String metricUploadClass = ConfigExtension.getMetricUploaderClass(nimbusData.getConf());
        if (StringUtils.isBlank(metricUploadClass)) {
            metricUploadClass = DefaultMetricUploader.class.getName();
        }
        // init metric uploader
        LOG.info("metric uploader classes:{}", metricUploadClass);
        String[] classes = metricUploadClass.split(",");
        for (String klass : classes) {
            klass = klass.trim();
            if (StringUtils.isBlank(klass)) {
                continue;
            }

            Object instance = Utils.newInstance(klass);
            if (!(instance instanceof MetricUploader)) {
                throw new RuntimeException(klass + " isn't MetricUploader class ");
            }
            MetricUploader metricUploader = (MetricUploader) instance;
            try {
                metricUploader.init(nimbusData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            metricUploaders.add(metricUploader);
            LOG.info("Successfully init metric uploaders:{}", metricUploaders);
        }
        this.lastMetricUploader = metricUploaders.get(metricUploaders.size() - 1);

        // init metric query client
        String metricQueryClientClass = ConfigExtension.getMetricQueryClientClass(nimbusData.getConf());
        if (!StringUtils.isBlank(metricQueryClientClass)) {
            LOG.info("metric query client class:{}", metricQueryClientClass);
            this.metricQueryClient = (MetricQueryClient) Utils.newInstance(metricQueryClientClass);
        } else {
            LOG.warn("use default metric query client class.");
            this.metricQueryClient = new DefaultMetricQueryClient();
        }
        try {
            metricQueryClient.init(nimbusData.getConf());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Successfully init MetricQureyClient ");

        this.metricUploaderDelegate = new MetricUploaderDelegate();
        this.readyToUpload.set(true);
    }

    public void pushRefreshEvent() {
        LOG.debug("Issue RefreshEvent.RefreshSys Event");

        RefreshEvent refreshTopologyEvent = new RefreshEvent();
        refreshTopologyEvent.setClusterMetricsContext(this);
        nimbusData.getScheduExec().scheduleAtFixedRate(refreshTopologyEvent, 0, 60, TimeUnit.SECONDS);
    }

    public void pushFlushEvent() {
        FlushEvent event = new FlushEvent();
        event.setClusterMetricsContext(this);
        nimbusData.getScheduExec().scheduleAtFixedRate(event, 15, 15, TimeUnit.SECONDS);
    }

    public void pushMergeEvent() {
        MergeEvent event = new MergeEvent();
        event.setClusterMetricsContext(this);

        nimbusData.getScheduExec().scheduleAtFixedRate(event, 60, 60, TimeUnit.SECONDS);
    }

    public void pushDiagnosisEvent() {
        DiagnosisEvent event = new DiagnosisEvent();
        event.setClusterMetricsContext(this);

        nimbusData.getScheduExec().scheduleAtFixedRate(event, 60, 60, TimeUnit.SECONDS);
    }

    public void pushUploadEvent() {
        UploadEvent event = new UploadEvent();
        event.setClusterMetricsContext(this);

        // special, upload thread is actually an inf-loop, so we use Long.MAX_VALUE
        nimbusData.getScheduExec().scheduleAtFixedRate(event, 0, Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    public void shutdown() {
        LOG.info("Begin to shutdown");
        getMetricUploaderDelegate().cleanup();

        LOG.info("Successfully shutdown");
    }

    /**
     * get topology metrics, note that only topology & component & worker
     * metrics are returned
     */
    public TopologyMetric getTopologyMetric(String topologyId) {
        long start = System.nanoTime();
        try {
            TopologyMetric ret = new TopologyMetric();
            List<MetricInfo> topologyMetrics = metricCache.getMetricData(topologyId, MetaType.TOPOLOGY);
            List<MetricInfo> componentMetrics = metricCache.getMetricData(topologyId, MetaType.COMPONENT);
            List<MetricInfo> workerMetrics = metricCache.getMetricData(topologyId, MetaType.WORKER);

            MetricInfo dummy = MetricUtils.mkMetricInfo();
            if (topologyMetrics.size() > 0) {
                // get the last min topology metric
                ret.set_topologyMetric(topologyMetrics.get(topologyMetrics.size() - 1));
            } else {
                ret.set_topologyMetric(dummy);
            }
            if (componentMetrics.size() > 0) {
                ret.set_componentMetric(componentMetrics.get(0));
            } else {
                ret.set_componentMetric(dummy);
            }
            if (workerMetrics.size() > 0) {
                ret.set_workerMetric(workerMetrics.get(0));
            } else {
                ret.set_workerMetric(dummy);
            }
            ret.set_taskMetric(dummy);
            ret.set_streamMetric(dummy);
            ret.set_nettyMetric(dummy);

            return ret;
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getTopologyMetric", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    public void deleteMetric(String topologyId, int metaType, List<String> idList) {
        final TopologyMetricContext context = topologyMetricContexts.get(topologyId);
        if (context != null) {
            for (String id : idList) {
                MetricMeta meta = metricQueryClient.getMetricMeta(clusterName, topologyId, MetaType.parse(metaType), id);
                if (meta != null) {
                    LOG.warn("deleting metric meta:{}", meta);
                    metricQueryClient.deleteMeta(meta);
                    context.getMemMeta().remove(meta.getFQN());

                    metricCache.put(topologyId, context.getMemMeta());
                } else {
                    LOG.warn("Failed to delete metric meta, topology:{}, metaType:{}, id:{}, meta not found",
                            topologyId, metaType, id);
                }
            }
        } else {
            LOG.warn("Failed to delete metric meta, topology:{} doesn't exist!", topologyId);
        }
    }

    public TopologyMetricContext getClusterTopologyMetricContext() {
        return topologyMetricContexts.get(JStormMetrics.CLUSTER_METRIC_KEY);
    }

    public static String getWorkerSlotName(String hostname, Integer port) {
        return hostname + ":" + port;
    }

    public boolean isTopologyAlive(String topologyId) {
        return topologyMetricContexts.containsKey(topologyId);
    }

    public TopologyMetric getMetricDataFromCache(int idx) {
        return (TopologyMetric) metricCache.get(PENDING_UPLOAD_METRIC_DATA + idx);
    }

    public TopologyMetricDataInfo getMetricDataInfoFromCache(int idx) {
        return (TopologyMetricDataInfo) metricCache.get(PENDING_UPLOAD_METRIC_DATA_INFO + idx);
    }

    public Map<String, Long> registerMetrics(String topologyId, Set<String> metricNames) {
        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);

        TopologyMetricContext topologyMetricContext = topologyMetricContexts.get(topologyId);
        if (topologyMetricContext == null) {
            LOG.warn("topology metrics context does not exist for topology:{}!!!", topologyId);
            return new HashMap<>();
        }

//        if (!topologyMetricContext.finishSyncRemote()) {
//            LOG.warn("waiting for topology {} to finish sync with remote.", topologyId);
//            return new HashMap<>();
//        }

        ConcurrentMap<String, Long> memMeta = topologyMetricContexts.get(topologyId).getMemMeta();
        Map<String, Long> ret = new HashMap<>();
        for (String metricName : metricNames) {
            Long id = memMeta.get(metricName);
            if (id != null && MetricUtils.isValidId(id)) {
                ret.put(metricName, id);
            } else {
                id = metricIDGenerator.genMetricId(metricName);
                Long old = memMeta.putIfAbsent(metricName, id);
                if (old == null) {
                    ret.put(metricName, id);
                } else {
                    ret.put(metricName, old);
                }
            }
        }
        long cost = ticker.stop();
        LOG.info("register metrics, topology:{}, size:{}, cost:{}", topologyId, metricNames.size(), cost);

        return ret;
    }

    public void printDiagnosticStats() {
        StringBuilder sb = new StringBuilder(256);
        for (int i = 0, j = 0; i < maxPendingUploadMetrics; i++) {
            int v = metricStat.get(i);
            if (v != UNSET) {
                sb.append(i).append(":").append(v).append("\t");
                if (++j % 5 == 0) {
                    sb.append("\n");
                }
            }
        }
        LOG.info("metric stats\n--------------------------------------\n{}\n",
                sb.length() == 0 ? "ALL UNSET" : sb.toString());
    }

    public int getAndPresetFirstEmptyIndex() {
        for (int i = 0; i < maxPendingUploadMetrics; i++) {
            if (metricStat.get(i) == UNSET) {
                if (metricStat.compareAndSet(i, UNSET, PRE_SET)) {
                    return i;
                }
            }
        }
        return -1;
    }

    public int getFirstPendingUploadIndex() {
        for (int i = 0; i < maxPendingUploadMetrics; i++) {
            if (metricStat.get(i) == SET) {
                return i;
            }
        }
        return -1;
    }

    public void markUploaded(MetricUploader curUploader, int idx) {
        if (curUploader == lastMetricUploader) {
            forceMarkUploaded(idx);
        }
    }

    public void forceMarkUploaded(int idx) {
        this.metricCache.remove(PENDING_UPLOAD_METRIC_DATA + idx);
        this.metricCache.remove(PENDING_UPLOAD_METRIC_DATA_INFO + idx);
        this.metricStat.set(idx, UNSET);
        if (MetricUploader.rateController.isEnableRateControl()) {
            MetricUploader.rateController.decrUploadingNum();
        }
    }

    public void markUploading(int idx) {
        this.metricStat.set(idx, UPLOADING);
    }

    public void markSet(int idx) {
        this.metricStat.set(idx, SET);
    }

    public JStormMetricCache getMetricCache() {
        return metricCache;
    }

    public ConcurrentMap<String, TopologyMetricContext> getTopologyMetricContexts() {
        return topologyMetricContexts;
    }

    public StormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public boolean isReadyToUpload() {
        return this.readyToUpload.get();
    }

    public MetricUploaderDelegate getMetricUploaderDelegate() {
        return metricUploaderDelegate;
    }

    public NimbusData getNimbusData() {
        return nimbusData;
    }

    public MetricQueryClient getMetricQueryClient() {
        return metricQueryClient;
    }

    public MetricIDGenerator getMetricIDGenerator() {
        return metricIDGenerator;
    }

    public String getClusterName() {
        return clusterName;
    }


    public class MetricUploaderDelegate {

        public boolean isUnderFlowControl() {
            return MetricUploader.rateController.isEnableRateControl() &&
                    !MetricUploader.rateController.syncToUpload();
        }

        public void cleanup() {
            for (MetricUploader metricUploader : metricUploaders) {
                metricUploader.cleanup();
            }
        }

        /**
         * register metrics to external metric plugin
         */
        public boolean registerMetrics(String clusterName, String topologyId, Map<String, Long> metrics) throws
                Exception {
            boolean ret = true;
            for (MetricUploader metricUploader : metricUploaders) {
                ret &= metricUploader.registerMetrics(clusterName, topologyId, metrics);
            }
            return ret;
        }

        /**
         * upload topologyMetric to external metric plugin (such as database plugin)
         *
         * @return true means success, false means failure
         */
        public boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric, Map<String, Object>
                metricContext) {
            boolean ret = true;
            for (MetricUploader metricUploader : metricUploaders) {
                ret &= metricUploader.upload(clusterName, topologyId, tpMetric, metricContext);
            }
            return ret;
        }

        /**
         * upload metrics with given key and metric context. the implementation can
         * retrieve metric data from rocks db
         * in the handler thread, which is kind of lazy-init, making it more
         * GC-friendly
         */
        public boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext) {
            boolean ret = true;
            for (MetricUploader metricUploader : metricUploaders) {
                ret &= metricUploader.upload(clusterName, topologyId, key, metricContext);
            }
            return ret;
        }

        /**
         * Send an event to underlying handler
         */
        public boolean sendEvent(String clusterName, MetricEvent event) {
            boolean ret = true;
            for (MetricUploader metricUploader : metricUploaders) {
                ret &= metricUploader.sendEvent(clusterName, event);
            }
            return ret;
        }

    }
}
