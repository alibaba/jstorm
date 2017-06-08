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

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.codahale.JAverageSnapshot;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A topology metric context contains all in-memory metric data of a topology.
 * This class resides in TopologyMaster.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class TopologyMetricContext {
    public static final Logger LOG = LoggerFactory.getLogger(TopologyMetricContext.class);

    private final ReentrantLock lock = new ReentrantLock();
    private Set<ResourceWorkerSlot> workerSet;
    private int taskNum = 1;
    private ConcurrentMap<String, MetricInfo> memCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> memMeta = new ConcurrentHashMap<>();
    private final AtomicBoolean isMerging = new AtomicBoolean(false);
    private String topologyId;
    private volatile int flushedMetaNum = 0;

    /**
     * sync meta from metric cache on startup
     */
    private volatile boolean syncMeta = false;

    private Map conf;

    public TopologyMetricContext() {
    }

    public TopologyMetricContext(Set<ResourceWorkerSlot> workerSet) {
        this.workerSet = workerSet;
    }

    public TopologyMetricContext(String topologyId, Set<ResourceWorkerSlot> workerSet, Map conf) {
        this(workerSet);
        this.topologyId = topologyId;
        this.conf = conf;
    }

    public ConcurrentMap<String, Long> getMemMeta() {
        return memMeta;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public boolean syncMeta() {
        return syncMeta;
    }

    public void setSyncMeta(boolean syncMeta) {
        this.syncMeta = syncMeta;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    public int getFlushedMetaNum() {
        return flushedMetaNum;
    }

    public void setFlushedMetaNum(int flushedMetaNum) {
        this.flushedMetaNum = flushedMetaNum;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public int getWorkerNum() {
        return workerSet.size();
    }

    public Set<ResourceWorkerSlot> getWorkerSet() {
        return workerSet;
    }

    public void setWorkerSet(Set<ResourceWorkerSlot> workerSet) {
        this.workerSet = workerSet;
    }

    public void resetUploadedMetrics() {
        this.memCache.clear();
    }

    public final ConcurrentMap<String, MetricInfo> getMemCache() {
        return memCache;
    }

    public void addToMemCache(String workerSlot, MetricInfo metricInfo) {
        memCache.put(workerSlot, metricInfo);
        LOG.info("update mem cache, worker:{}, total uploaded:{}", workerSlot, memCache.size());
    }

    public boolean readyToUpload() {
        return memCache.size() >= workerSet.size();
    }

    public boolean isMerging() {
        return isMerging.get();
    }

    public void setMerging(boolean isMerging) {
        this.isMerging.set(isMerging);
    }

    public int getUploadedWorkerNum() {
        return memCache.size();
    }

    public TopologyMetric mergeMetrics() {
        long start = System.currentTimeMillis();

        if (getMemCache().size() == 0) {
            //LOG.info("topology:{}, metric size is 0, skip...", topologyId);
            return null;
        }
        if (isMerging()) {
            LOG.info("topology {} is already merging, skip...", topologyId);
            return null;
        }

        setMerging(true);

        try {
            Map<String, MetricInfo> workerMetricMap = this.memCache;
            // reset mem cache
            this.memCache = new ConcurrentHashMap<>();

            MetricInfo topologyMetrics = MetricUtils.mkMetricInfo();
            MetricInfo componentMetrics = MetricUtils.mkMetricInfo();
            MetricInfo compStreamMetrics = MetricUtils.mkMetricInfo();
            MetricInfo taskMetrics = MetricUtils.mkMetricInfo();
            MetricInfo streamMetrics = MetricUtils.mkMetricInfo();
            MetricInfo workerMetrics = MetricUtils.mkMetricInfo();
            MetricInfo nettyMetrics = MetricUtils.mkMetricInfo();
            TopologyMetric tpMetric = new TopologyMetric(
                    topologyMetrics, componentMetrics, workerMetrics, taskMetrics, streamMetrics, nettyMetrics);
            tpMetric.set_compStreamMetric(compStreamMetrics);


            // metric name => worker count
            Map<String, Integer> histogramMetricNameCounters = new HashMap<>();

            // special for histograms & timers, we merge the points to get a new snapshot data.
            Map<String, Map<Integer, Histogram>> histograms = new HashMap<>();

            // iterate metrics of all workers within the same topology
            for (ConcurrentMap.Entry<String, MetricInfo> metricEntry : workerMetricMap.entrySet()) {
                MetricInfo metricInfo = metricEntry.getValue();

                // merge counters: add old and new values, note we only add incoming new metrics and overwrite
                // existing data, same for all below.
                Map<String, Map<Integer, MetricSnapshot>> metrics = metricInfo.get_metrics();
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : metrics.entrySet()) {
                    String metricName = metric.getKey();
                    Map<Integer, MetricSnapshot> data = metric.getValue();
                    MetaType metaType = MetricUtils.metaType(metricName);

                    MetricType metricType = MetricUtils.metricType(metricName);
                    if (metricType == MetricType.COUNTER) {
                        mergeCounters(tpMetric, metaType, metricName, data);
                    } else if (metricType == MetricType.GAUGE) {
                        mergeGauges(tpMetric, metaType, metricName, data);
                    } else if (metricType == MetricType.METER) {
                        mergeMeters(getMetricInfoByType(tpMetric, metaType), metricName, data);
                    } else if (metricType == MetricType.HISTOGRAM) {
                        mergeHistograms(getMetricInfoByType(tpMetric, metaType),
                                metricName, data, histogramMetricNameCounters, histograms);
                    }
                }
            }
            adjustHistogramTimerMetrics(tpMetric, histogramMetricNameCounters, histograms);
            // for counters, we only report delta data every time, need to sum with old data
            //adjustCounterMetrics(tpMetric, oldTpMetric);

            LOG.info("merge topology metrics:{}, cost:{}", topologyId, System.currentTimeMillis() - start);
            LOG.debug("tp:{}, comp:{}, comp_stream:{}, task:{}, stream:{}, worker:{}, netty:{}",
                    topologyMetrics.get_metrics_size(), componentMetrics.get_metrics_size(),
                    compStreamMetrics.get_metrics_size(), taskMetrics.get_metrics_size(),
                    streamMetrics.get_metrics_size(), workerMetrics.get_metrics_size(),
                    nettyMetrics.get_metrics_size());
            return tpMetric;
        } finally {
            setMerging(false);
        }
    }


    protected MetricInfo getMetricInfoByType(TopologyMetric topologyMetric, MetaType type) {
        if (type == MetaType.TASK) {
            return topologyMetric.get_taskMetric();
        } else if (type == MetaType.WORKER) {
            return topologyMetric.get_workerMetric();
        } else if (type == MetaType.COMPONENT) {
            return topologyMetric.get_componentMetric();
        } else if (type == MetaType.STREAM) {
            return topologyMetric.get_streamMetric();
        } else if (type == MetaType.NETTY) {
            return topologyMetric.get_nettyMetric();
        } else if (type == MetaType.TOPOLOGY) {
            return topologyMetric.get_topologyMetric();
        } else if (type == MetaType.COMPONENT_STREAM) {
            return topologyMetric.get_compStreamMetric();
        }
        return null;
    }

    public void mergeCounters(TopologyMetric tpMetric, MetaType metaType, String meta,
                              Map<Integer, MetricSnapshot> data) {
        MetricInfo metricInfo = getMetricInfoByType(tpMetric, metaType);
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                } else {
                    old.set_ts(snapshot.get_ts());
                    old.set_longValue(old.get_longValue() + snapshot.get_longValue());
                }
            }
        }
    }

    public void mergeGauges(TopologyMetric tpMetric, MetaType metaType, String meta,
                            Map<Integer, MetricSnapshot> data) {
        MetricInfo metricInfo = getMetricInfoByType(tpMetric, metaType);
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        // add gauge values anyway
                        old.set_doubleValue(old.get_doubleValue() + snapshot.get_doubleValue());

                        //if (metaType != MetaType.TOPOLOGY) {
                        //    old.set_doubleValue(snapshot.get_doubleValue());
                        //} else { // for topology metric, gauge might be add-able, e.g., cpu, memory, etc.
                        //    old.set_doubleValue(old.get_doubleValue() + snapshot.get_doubleValue());
                        //}
                    }
                }
            }
        }
    }

    /**
     * meters are not sampled.
     */
    public void mergeMeters(MetricInfo metricInfo, String meta, Map<Integer, MetricSnapshot> data) {
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        old.set_mean(old.get_mean() + snapshot.get_mean());
                        old.set_m1(old.get_m1() + snapshot.get_m1());
                        old.set_m5(old.get_m5() + snapshot.get_m5());
                        old.set_m15(old.get_m15() + snapshot.get_m15());
                    }
                }
            }
        }
    }

    /**
     * histograms are sampled, but we just update points
     */
    public void mergeHistograms(MetricInfo metricInfo, String meta, Map<Integer, MetricSnapshot> data,
                                Map<String, Integer> metaCounters, Map<String, Map<Integer, Histogram>> histograms) {
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
            Map<Integer, Histogram> histogramMap = new HashMap<>();
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Histogram histogram = MetricUtils.metricSnapshot2Histogram(dataEntry.getValue());
                histogramMap.put(dataEntry.getKey(), histogram);
            }
            histograms.put(meta, histogramMap);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                    histograms.get(meta).put(win, MetricUtils.metricSnapshot2Histogram(snapshot));
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        Histogram histogram = histograms.get(meta).get(win);
                        Snapshot updateSnapshot = histogram.getSnapshot();
                        if (updateSnapshot instanceof JAverageSnapshot) {
                            sumMetricSnapshot(((JAverageSnapshot) updateSnapshot).getMetricSnapshot(), snapshot);
                        } else {
                            // update points
                            MetricUtils.updateHistogramPoints(histogram, snapshot.get_points(), snapshot.get_pointSize());
                        }
                    }
                }
            }
        }
        updateMetricCounters(meta, metaCounters);
    }

    /**
     * sum histograms
     */
    public void sumMetricSnapshot(MetricSnapshot metricSnapshot, MetricSnapshot snapshot) {
        metricSnapshot.set_min(metricSnapshot.get_min() + snapshot.get_min());
        metricSnapshot.set_max(metricSnapshot.get_max() + snapshot.get_max());
        metricSnapshot.set_p50(metricSnapshot.get_p50() + snapshot.get_p50());
        metricSnapshot.set_p75(metricSnapshot.get_p75() + snapshot.get_p75());
        metricSnapshot.set_p95(metricSnapshot.get_p95() + snapshot.get_p95());
        metricSnapshot.set_p98(metricSnapshot.get_p98() + snapshot.get_p98());
        metricSnapshot.set_p99(metricSnapshot.get_p99() + snapshot.get_p99());
        metricSnapshot.set_p999(metricSnapshot.get_p999() + snapshot.get_p999());
        metricSnapshot.set_mean(metricSnapshot.get_mean() + snapshot.get_mean());
        metricSnapshot.set_stddev(metricSnapshot.get_stddev() + snapshot.get_stddev());
    }


    /**
     * computes occurrences of specified metric name
     */
    protected void updateMetricCounters(String metricName, Map<String, Integer> metricNameCounters) {
        if (metricNameCounters.containsKey(metricName)) {
            metricNameCounters.put(metricName, metricNameCounters.get(metricName) + 1);
        } else {
            metricNameCounters.put(metricName, 1);
        }
    }

    protected void adjustHistogramTimerMetrics(TopologyMetric tpMetric, Map<String, Integer> metaCounters,
                                               Map<String, Map<Integer, Histogram>> histograms) {
        resetPoints(tpMetric.get_taskMetric().get_metrics());
        resetPoints(tpMetric.get_streamMetric().get_metrics());
        resetPoints(tpMetric.get_nettyMetric().get_metrics());
        resetPoints(tpMetric.get_workerMetric().get_metrics());

        Map<String, Map<Integer, MetricSnapshot>> compMetrics = tpMetric.get_componentMetric().get_metrics();
        Map<String, Map<Integer, MetricSnapshot>> topologyMetrics = tpMetric.get_topologyMetric().get_metrics();

        adjustMetrics(compMetrics, metaCounters, histograms);
        adjustMetrics(topologyMetrics, metaCounters, histograms);
    }

    private void adjustMetrics(Map<String, Map<Integer, MetricSnapshot>> metrics, Map<String, Integer> metaCounters,
                               Map<String, Map<Integer, Histogram>> histograms) {
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metrics.entrySet()) {
            String meta = metricEntry.getKey();
            MetricType metricType = MetricUtils.metricType(meta);
            MetaType metaType = MetricUtils.metaType(meta);
            Map<Integer, MetricSnapshot> winData = metricEntry.getValue();

            if (metricType == MetricType.HISTOGRAM) {
                for (Map.Entry<Integer, MetricSnapshot> dataEntry : winData.entrySet()) {
                    MetricSnapshot snapshot = dataEntry.getValue();
                    Integer cnt = metaCounters.get(meta);
                    Histogram histogram = histograms.get(meta).get(dataEntry.getKey());
                    if (cnt != null && cnt > 1) {
                        int denominator = 1;
                        if (!MetricUtils.metricAccurateCal)
                            denominator = cnt;
                        Snapshot snapshot1 = histogram.getSnapshot();
                        snapshot.set_mean(snapshot1.getMean() / denominator);
                        snapshot.set_p50(snapshot1.getMedian() / denominator);
                        snapshot.set_p75(snapshot1.get75thPercentile() / denominator);
                        snapshot.set_p95(snapshot1.get95thPercentile() / denominator);
                        snapshot.set_p98(snapshot1.get98thPercentile() / denominator);
                        snapshot.set_p99(snapshot1.get99thPercentile() / denominator);
                        snapshot.set_p999(snapshot1.get999thPercentile() / denominator);
                        snapshot.set_stddev(snapshot1.getStdDev() / denominator);
                        snapshot.set_min(snapshot1.getMin() / denominator);
                        snapshot.set_max(snapshot1.getMax() / denominator);

                        if (MetricUtils.metricAccurateCal && metaType == MetaType.TOPOLOGY) {
                            snapshot.set_points(MetricUtils.longs2bytes(snapshot1.getValues()));
                        }
                    }
                    if (metaType != MetaType.TOPOLOGY || !MetricUtils.metricAccurateCal) {
                        snapshot.set_points(new byte[0]);
                    }
                }

            }
        }
    }

    private void resetPoints(Map<String, Map<Integer, MetricSnapshot>> metrics) {
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metrics.entrySet()) {
            String meta = metricEntry.getKey();
            MetricType metricType = MetricUtils.metricType(meta);
            Map<Integer, MetricSnapshot> winData = metricEntry.getValue();

            if (metricType == MetricType.HISTOGRAM) {
                for (MetricSnapshot snapshot : winData.values()) {
                    snapshot.set_points(new byte[0]);
                }
            }
        }
    }

    protected void adjustCounterMetrics(TopologyMetric tpMetric, TopologyMetric oldMetric) {
        if (oldMetric != null) {
            mergeCounters(tpMetric.get_streamMetric().get_metrics(),
                    oldMetric.get_streamMetric().get_metrics());

            mergeCounters(tpMetric.get_taskMetric().get_metrics(),
                    oldMetric.get_taskMetric().get_metrics());

            mergeCounters(tpMetric.get_componentMetric().get_metrics(),
                    oldMetric.get_componentMetric().get_metrics());

            mergeCounters(tpMetric.get_workerMetric().get_metrics(),
                    oldMetric.get_workerMetric().get_metrics());

            mergeCounters(tpMetric.get_nettyMetric().get_metrics(),
                    oldMetric.get_nettyMetric().get_metrics());
        }
    }

    /**
     * sum old counter snapshots and new counter snapshots, sums are stored in new snapshots.
     */
    private void mergeCounters(Map<String, Map<Integer, MetricSnapshot>> newCounters,
                               Map<String, Map<Integer, MetricSnapshot>> oldCounters) {
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : newCounters.entrySet()) {
            String metricName = entry.getKey();
            Map<Integer, MetricSnapshot> snapshots = entry.getValue();
            Map<Integer, MetricSnapshot> oldSnapshots = oldCounters.get(metricName);
            if (oldSnapshots != null && oldSnapshots.size() > 0) {
                for (Map.Entry<Integer, MetricSnapshot> snapshotEntry : snapshots.entrySet()) {
                    Integer win = snapshotEntry.getKey();
                    MetricSnapshot snapshot = snapshotEntry.getValue();
                    MetricSnapshot oldSnapshot = oldSnapshots.get(win);
                    if (oldSnapshot != null) {
                        snapshot.set_longValue(snapshot.get_longValue() + oldSnapshot.get_longValue());
                    }
                }
            }
        }
    }

    private double getSampleRate() {
        return ConfigExtension.getMetricSampleRate(conf);
    }

}
