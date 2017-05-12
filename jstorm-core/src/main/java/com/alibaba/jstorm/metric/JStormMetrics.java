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
import com.alibaba.jstorm.common.metric.*;
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
@SuppressWarnings({"unused", "unchecked"})
public class JStormMetrics implements Serializable {
    private static final long serialVersionUID = -2580242512743243267L;

    public static final String NIMBUS_METRIC_KEY = "__NIMBUS__";
    public static final String CLUSTER_METRIC_KEY = "__CLUSTER__";
    public static final String SUPERVISOR_METRIC_KEY = "__SUPERVISOR__";

    public static final String[] SYS_TOPOLOGIES = {
            NIMBUS_METRIC_KEY, CLUSTER_METRIC_KEY, SUPERVISOR_METRIC_KEY
    };

    public static final Set<String> SYS_TOPOLOGY_SET = Sets.newHashSet(SYS_TOPOLOGIES);

    public static final String DEFAULT_GROUP = "sys";
    public static final String NETTY_GROUP = "netty";

    protected static final Logger LOG = LoggerFactory.getLogger(JStormMetrics.class);

    /**
     * Metrics in this object will be uploaded to nimbus
     */
    protected static final AsmMetricRegistry workerMetrics = new AsmMetricRegistry();
    protected static final AsmMetricRegistry nettyMetrics = new AsmMetricRegistry();
    protected static final AsmMetricRegistry componentMetrics = new AsmMetricRegistry();
    protected static final AsmMetricRegistry compStreamMetrics = new AsmMetricRegistry();
    protected static final AsmMetricRegistry taskMetrics = new AsmMetricRegistry();
    protected static final AsmMetricRegistry streamMetrics = new AsmMetricRegistry();
    protected static final AsmMetricRegistry topologyMetrics = new AsmMetricRegistry();

    protected static final AsmMetricRegistry[] allRegistries = {
            streamMetrics, taskMetrics, componentMetrics, compStreamMetrics,
            workerMetrics, nettyMetrics, topologyMetrics};

    protected static String topologyId;
    protected static String host;
    protected static int port;
    protected static boolean debug;

    protected static final Set<String> debugMetricNames = new HashSet<>();
    protected static final Set<String> disabledMetricNames = new HashSet<>();

    protected static int histogramValueSize;

    static {
        host = NetWorkUtils.ip();
    }

    public static void setHistogramValueSize(int histogramValueSize) {
        JStormMetrics.histogramValueSize = histogramValueSize;
    }

    public static volatile boolean enabled = true;
    public static volatile boolean enableStreamMetrics = true;

    public static void setTimerUpdateInterval(long interval) {
        AsmHistogram.setUpdateInterval(interval);
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        JStormMetrics.port = port;
    }

    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        JStormMetrics.host = host;
    }

    public static String getTopologyId() {
        return topologyId;
    }

    public static void setTopologyId(String topologyId) {
        JStormMetrics.topologyId = topologyId;
    }

    public static boolean isDebug() {
        return debug;
    }

    public static void setDebug(boolean debug) {
        JStormMetrics.debug = debug;
        LOG.info("topology metrics debug enabled:{}", debug);
    }

    public static String workerMetricName(String name, MetricType type) {
        return MetricUtils.workerMetricName(topologyId, host, port, name, type);
    }

    public static void addDebugMetrics(String names) {
        if (names == null) {
            return;
        }
        String[] metrics = names.split(",");
        for (String metric : metrics) {
            metric = metric.trim();
            if (!StringUtils.isBlank(metric)) {
                debugMetricNames.add(metric);
            }
        }
        LOG.info("debug metric names:{}", Joiner.on(",").join(debugMetricNames));
    }

    public static void updateDisabledMetrics(String names) {
        if (names == null) {
            return;
        }
        disabledMetricNames.clear();
        String[] metrics = names.split(",");
        for (String metric : metrics) {
            metric = metric.trim();
            if (!StringUtils.isBlank(metric)) {
                disabledMetricNames.add(metric);
            }
        }
        LOG.info("disabled metric names:{}", Joiner.on(",").join(disabledMetricNames));
    }

    /**
     * reserve for debug purposes
     */
    public static AsmMetric find(String name) {
        for (AsmMetricRegistry registry : allRegistries) {
            AsmMetric metric = registry.getMetric(name);
            if (metric != null) {
                return metric;
            }
        }
        return null;
    }

    public static List<AsmMetric> search(String metricName, MetaType metaType, MetricType metricType) {
        if (metaType == MetaType.TOPOLOGY) {
            return search(topologyMetrics, metricName, metricType);
        } else if (metaType == MetaType.COMPONENT) {
            return search(componentMetrics, metricName, metricType);
        } else if (metaType == MetaType.TASK) {
            return search(taskMetrics, metricName, metricType);
        } else if (metaType == MetaType.COMPONENT_STREAM) {
            return search(compStreamMetrics, metricName, metricType);
        } else if (metaType == MetaType.WORKER) {
            return search(workerMetrics, metricName, metricType);
        }
        return Lists.newArrayList();
    }

    private static List<AsmMetric> search(AsmMetricRegistry registry, String metricName, MetricType metricType) {
        List<AsmMetric> ret = Lists.newArrayList();

        Map<String, ?> metricMap;
        if (metricType == MetricType.COUNTER) {
            metricMap = registry.getCounters();
        } else if (metricType == MetricType.GAUGE) {
            metricMap = registry.getGauges();
        } else if (metricType == MetricType.METER) {
            metricMap = registry.getMeters();
        } else {
            metricMap = registry.getHistograms();
        }
        for (Map.Entry<String, ?> entry : metricMap.entrySet()) {
            if (entry.getKey().endsWith(metricName)) {
                ret.add((AsmMetric) entry.getValue());
            }
        }

        return ret;
    }

    public static AsmMetric registerStreamMetric(String name, AsmMetric metric, boolean mergeTopology) {
        name = fixNameIfPossible(name);
        LOG.debug("register stream metric:{}", name);
        AsmMetric ret = streamMetrics.register(name, metric);
        //ret.setEnabled(enableStreamMetrics);

        if (metric.isAggregate()) {
            List<AsmMetric> assocMetrics = new ArrayList<>();

            // in this step we've normalized metric names which contains "."
            // e.g., a metric named: 'abc.def' will be normalized to 'def'
            String taskMetricName = MetricUtils.stream2taskName(name);
            AsmMetric taskMetric = taskMetrics.register(taskMetricName, metric.clone());
            assocMetrics.add(taskMetric);

            String compMetricName = MetricUtils.task2compName(taskMetricName);
            AsmMetric componentMetric = componentMetrics.register(compMetricName, taskMetric.clone());
            assocMetrics.add(componentMetric);

            String compStreamMetricName = MetricUtils.stream2compStreamName(name);
            AsmMetric componentStreamMetric = compStreamMetrics.register(compStreamMetricName, ret.clone());
            assocMetrics.add(componentStreamMetric);

            if (mergeTopology) {
                String topologyMetricName = MetricUtils.comp2topologyName(compMetricName);
                AsmMetric topologyMetric = topologyMetrics.register(topologyMetricName, ret.clone());
                assocMetrics.add(topologyMetric);
            }

            ret.addAssocMetrics(assocMetrics.toArray(new AsmMetric[assocMetrics.size()]));
        }

        return ret;
    }

    public static AsmMetric registerTaskMetric(String name, AsmMetric metric) {
        name = fixNameIfPossible(name);
        AsmMetric ret = taskMetrics.register(name, metric);

        if (metric.isAggregate()) {
            String compMetricName = MetricUtils.task2compName(name);
            AsmMetric componentMetric = componentMetrics.register(compMetricName, ret.clone());

            ret.addAssocMetrics(componentMetric);
        }

        return ret;
    }

    public static AsmMetric registerTaskTopologyMetric(String name, AsmMetric metric) {
        name = fixNameIfPossible(name);
        AsmMetric ret = taskMetrics.register(name, metric);

        String compMetricName = MetricUtils.task2compName(name);
        AsmMetric compMetric = topologyMetrics.register(compMetricName, ret.clone());

        String topologyMetricName = MetricUtils.comp2topologyName(compMetricName);
        AsmMetric topologyMetric = topologyMetrics.register(topologyMetricName, ret.clone());

        ret.addAssocMetrics(compMetric, topologyMetric);
        return ret;
    }

    public static AsmMetric registerWorkerMetric(String name, AsmMetric metric) {
        name = fixNameIfPossible(name);
        return workerMetrics.register(name, metric);
    }

    public static AsmMetric registerWorkerTopologyMetric(String name, AsmMetric metric) {
        name = fixNameIfPossible(name);
        AsmMetric ret = workerMetrics.register(name, metric);

        String topologyMetricName = MetricUtils.worker2topologyName(name);
        AsmMetric topologyMetric = topologyMetrics.register(topologyMetricName, ret.clone());

        ret.addAssocMetrics(topologyMetric);

        return ret;
    }

    public static AsmMetric registerNettyMetric(String name, AsmMetric metric) {
        name = fixNameIfPossible(name, NETTY_GROUP);
        return nettyMetrics.register(name, metric);
    }

    /**
     * simplified helper method to register a worker histogram
     *
     * @param topologyId topology id
     * @param name       metric name, NOTE it's not a full-qualified name.
     * @param histogram  histogram
     * @return registered histogram
     */
    public static AsmHistogram registerWorkerHistogram(String topologyId, String name, AsmHistogram histogram) {
        return (AsmHistogram) registerWorkerMetric(
                MetricUtils.workerMetricName(topologyId, host, 0, name, MetricType.HISTOGRAM), histogram);
    }

    /**
     * simplified helper method to register a worker gauge
     */
    public static AsmGauge registerWorkerGauge(String topologyId, String name, AsmGauge gauge) {
        return (AsmGauge) registerWorkerMetric(
                MetricUtils.workerMetricName(topologyId, host, 0, name, MetricType.GAUGE), gauge);
    }

    /**
     * simplified helper method to register a worker meter
     */
    public static AsmMeter registerWorkerMeter(String topologyId, String name, AsmMeter meter) {
        return (AsmMeter) registerWorkerMetric(
                MetricUtils.workerMetricName(topologyId, host, 0, name, MetricType.METER), meter);
    }

    /**
     * simplified helper method to register a worker counter
     */
    public static AsmCounter registerWorkerCounter(String topologyId, String name, AsmCounter counter) {
        return (AsmCounter) registerWorkerMetric(
                MetricUtils.workerMetricName(topologyId, host, 0, name, MetricType.COUNTER), counter);
    }

    public static AsmMetric getStreamMetric(String name) {
        name = fixNameIfPossible(name);
        return streamMetrics.getMetric(name);
    }

    public static AsmMetric getTaskMetric(String name) {
        name = fixNameIfPossible(name);
        return taskMetrics.getMetric(name);
    }

    public static AsmMetric getComponentMetric(String name) {
        name = fixNameIfPossible(name);
        return componentMetrics.getMetric(name);
    }

    public static AsmMetric getWorkerMetric(String name) {
        name = fixNameIfPossible(name);
        return workerMetrics.getMetric(name);
    }

    public static void unregisterWorkerMetric(String name) {
        name = fixNameIfPossible(name);
        workerMetrics.remove(name);
    }

    public static void unregisterNettyMetric(String name) {
        name = fixNameIfPossible(name, NETTY_GROUP);
        nettyMetrics.remove(name);
    }

    public static void unregisterTaskMetric(String name) {
        name = fixNameIfPossible(name);
        taskMetrics.remove(name);
    }

    public static AsmMetricRegistry getNettyMetrics() {
        return nettyMetrics;
    }

    public static AsmMetricRegistry getWorkerMetrics() {
        return workerMetrics;
    }

    public static AsmMetricRegistry getComponentMetrics() {
        return componentMetrics;
    }

    public static AsmMetricRegistry getTaskMetrics() {
        return taskMetrics;
    }

    public static AsmMetricRegistry getStreamMetrics() {
        return streamMetrics;
    }

    public static AsmMetricRegistry getTopologyMetrics() {
        return topologyMetrics;
    }

    /**
     * convert snapshots to thrift objects, note that timestamps are aligned to min during the conversion,
     * so nimbus server will get snapshots with aligned timestamps (still in ms as TDDL will use it).
     */
    public static MetricInfo computeAllMetrics() {
        long start = System.currentTimeMillis();
        MetricInfo metricInfo = MetricUtils.mkMetricInfo();

        List<Map.Entry<String, AsmMetric>> entries = Lists.newLinkedList();
        if (enableStreamMetrics) {
            entries.addAll(streamMetrics.metrics.entrySet());
        }
        entries.addAll(taskMetrics.metrics.entrySet());
        entries.addAll(componentMetrics.metrics.entrySet());
        entries.addAll(compStreamMetrics.metrics.entrySet());
        entries.addAll(workerMetrics.metrics.entrySet());
        entries.addAll(nettyMetrics.metrics.entrySet());
        entries.addAll(topologyMetrics.metrics.entrySet());

        for (Map.Entry<String, AsmMetric> entry : entries) {
            String name = entry.getKey();
            AsmMetric metric = entry.getValue();

            // skip disabled metrics, double check
            if (disabledMetricNames.contains(metric.getShortName())) {
                continue;
            }
            Map<Integer, AsmSnapshot> snapshots = metric.getSnapshots();
            if (snapshots.size() == 0) {
                continue;
            }

            int op = metric.getOp();
            if ((op & AsmMetric.MetricOp.LOG) == AsmMetric.MetricOp.LOG) {
                MetricUtils.printMetricSnapshot(metric, snapshots);
            }

            if ((op & AsmMetric.MetricOp.REPORT) == AsmMetric.MetricOp.REPORT) {
                MetaType metaType = MetricUtils.metaType(metric.getMetricName());
                try {
                    if (metric instanceof AsmCounter) {
                        Map data = MetricUtils.toThriftCounterSnapshots(snapshots);
                        putIfNotEmpty(metricInfo.get_metrics(), name, data);
                    } else if (metric instanceof AsmGauge) {
                        Map data = MetricUtils.toThriftGaugeSnapshots(snapshots);
                        putIfNotEmpty(metricInfo.get_metrics(), name, data);
                    } else if (metric instanceof AsmMeter) {
                        Map data = MetricUtils.toThriftMeterSnapshots(snapshots);
                        putIfNotEmpty(metricInfo.get_metrics(), name, data);
                    } else if (metric instanceof AsmHistogram) {
                        Map data = MetricUtils.toThriftHistoSnapshots(metaType, snapshots);
                        putIfNotEmpty(metricInfo.get_metrics(), name, data);
                    }
                } catch (Exception ex) {
                    LOG.error("Error", ex);
                }
            }
        }

        if (debug) {
            MetricUtils.printMetricInfo(metricInfo, debugMetricNames);
        }
        LOG.debug("compute all metrics, cost:{}", System.currentTimeMillis() - start);
        return metricInfo;
    }

    public static MetricInfo approximateComputeAllMetrics() {
        long start = System.currentTimeMillis();

        // metric name => worker count
        Map<String, Integer> histogramMetricNameCounters = new HashMap<>();
        MetricInfo metricInfo = MetricUtils.mkMetricInfo();
        Map<String, Map<Integer, MetricSnapshot>> mergeWorkerMetrics = metricInfo.get_metrics();
        mergeLevelMetricSnapshot(mergeWorkerMetrics, streamMetrics.metrics, histogramMetricNameCounters);
        mergeLevelMetricSnapshot(mergeWorkerMetrics, taskMetrics.metrics, histogramMetricNameCounters);
        mergeLevelMetricSnapshot(mergeWorkerMetrics, componentMetrics.metrics, histogramMetricNameCounters);
        mergeLevelMetricSnapshot(mergeWorkerMetrics, compStreamMetrics.metrics, histogramMetricNameCounters);
        mergeLevelMetricSnapshot(mergeWorkerMetrics, workerMetrics.metrics, histogramMetricNameCounters);
        mergeLevelMetricSnapshot(mergeWorkerMetrics, nettyMetrics.metrics, histogramMetricNameCounters);
        mergeLevelMetricSnapshot(mergeWorkerMetrics, topologyMetrics.metrics, histogramMetricNameCounters);

        if (debug) {
            MetricUtils.printMetricInfo(metricInfo, debugMetricNames);
        }
        Set<String> filteredStreamNames = new HashSet<>();
        if (!enableStreamMetrics) {
            for (Map.Entry<String, AsmMetric> entry : streamMetrics.metrics.entrySet()) {
                filteredStreamNames.add(entry.getKey());
            }
        }
        Map<String, Map<Integer, MetricSnapshot>> uploadWorkerMetrics = new HashMap<>();
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : mergeWorkerMetrics.entrySet()) {
            if (!filteredStreamNames.contains(entry.getKey())) {
                Integer count = histogramMetricNameCounters.get(entry.getKey());
                Map<Integer, MetricSnapshot> values = entry.getValue();
                //adjust Histogram
                if (values != null && count != null && count > 1) {
                    for (Map.Entry<Integer, MetricSnapshot> entry1 : values.entrySet()) {
                        MetricSnapshot metricSnapshot = entry1.getValue();
                        metricSnapshot.set_min(metricSnapshot.get_min() / count);
                        metricSnapshot.set_max(metricSnapshot.get_max() / count);
                        metricSnapshot.set_p50(metricSnapshot.get_p50() / count);
                        metricSnapshot.set_p75(metricSnapshot.get_p75() / count);
                        metricSnapshot.set_p95(metricSnapshot.get_p95() / count);
                        metricSnapshot.set_p98(metricSnapshot.get_p98() / count);
                        metricSnapshot.set_p99(metricSnapshot.get_p99() / count);
                        metricSnapshot.set_p999(metricSnapshot.get_p999() / count);
                        metricSnapshot.set_mean(metricSnapshot.get_mean() / count);
                        metricSnapshot.set_stddev(metricSnapshot.get_stddev() / count);
                    }
                }
                uploadWorkerMetrics.put(entry.getKey(), entry.getValue());
            }

        }
        metricInfo.set_metrics(uploadWorkerMetrics);
        LOG.debug("approximately compute all metrics, cost:{}", System.currentTimeMillis() - start);

        return metricInfo;
    }

    public static void mergeLevelMetricSnapshot(Map<String, Map<Integer, MetricSnapshot>> mergeWorkerMetrics,
                                                ConcurrentMap<String, AsmMetric> metrics,
                                                Map<String, Integer> histogramMetricNameCounters) {

        for (Map.Entry<String, AsmMetric> entry : metrics.entrySet()) {
            String name = entry.getKey();
            AsmMetric metric = entry.getValue();
            if (metric.isAttached() || disabledMetricNames.contains(metric.getShortName()) ||
                    metric.getSnapshots().size() == 0) {
                continue;
            }

            Set<AsmMetric> assocMetrics = metric.getAssocMetrics();
            MetricType metricType = MetricUtils.metricType(metric.getMetricName());

            List<AsmMetric> asmMetricList = Lists.newLinkedList(assocMetrics);
            asmMetricList.add(metric);

            // snapshot of the root metric: stream/task
            Map<Integer, MetricSnapshot> rootSnapshotMap = MetricUtils.toThriftSnapshots(metric.getSnapshots(), metricType);

            for (AsmMetric asmMetric : asmMetricList) {
                int op = metric.getOp();
                if ((op & AsmMetric.MetricOp.LOG) == AsmMetric.MetricOp.LOG) {
                    MetricUtils.printMetricSnapshot(asmMetric, metric.getSnapshots());
                }
                if ((op & AsmMetric.MetricOp.REPORT) == AsmMetric.MetricOp.REPORT) {
                    try {
                        String metricName = asmMetric.getMetricName();
                        Map<Integer, MetricSnapshot> oldSnapshotMap = mergeWorkerMetrics.get(metricName);
                        if (oldSnapshotMap == null) {
                            mergeWorkerMetrics.put(metricName,
                                    copyMetricSnapshotMapWithId(rootSnapshotMap, asmMetric.getMetricId()));
                        } else {
                            MetricUtils.mergeMetricSnapshotMap(oldSnapshotMap, rootSnapshotMap, metricType);
                        }
                        if (metricType == MetricType.HISTOGRAM) {
                            if (histogramMetricNameCounters.containsKey(metricName)) {
                                histogramMetricNameCounters.put(metricName, histogramMetricNameCounters.get(metricName) + 1);
                            } else {
                                histogramMetricNameCounters.put(metricName, 1);
                            }
                        }
                    } catch (Exception ex) {
                        LOG.error("Error", ex);
                    }
                }
            }
        }
    }

    private static Map<Integer, MetricSnapshot> copyMetricSnapshotMapWithId(Map<Integer, MetricSnapshot> snapshotMap,
                                                                            Long metricId) {
        Map<Integer, MetricSnapshot> ret = new HashMap<>();
        for (Map.Entry<Integer, MetricSnapshot> entry : snapshotMap.entrySet()) {
            MetricSnapshot snapshot = new MetricSnapshot(entry.getValue());
            snapshot.set_metricId(metricId);
            ret.put(entry.getKey(), snapshot);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Map> void putIfNotEmpty(Map base, String name, T data) {
        if (data != null && data.size() > 0) {
            base.put(name, data);
        }
    }

    public static String fixNameIfPossible(String name) {
        return fixNameIfPossible(name, DEFAULT_GROUP);
    }

    public static String fixNameIfPossible(String name, String group) {
        MetaType type = MetricUtils.metaType(name);
        String[] parts = name.split(MetricUtils.DELIM);
        if (parts[1].equals("")) {
            parts[1] = topologyId;
        }
        if (type != MetaType.WORKER && parts[5].equals("")) {
            parts[5] = group;
        } else if (parts[2].equals("")) {
            parts[2] = host;
            parts[3] = port + "";
            if (parts[4].equals("")) {
                parts[4] = group;
            }
        }
        return MetricUtils.concat(parts);
    }

    public static void main(String[] args) throws Exception {
        JStormMetrics.topologyId = "topologyId";
        JStormMetrics.host = "127.0.0.1";
        JStormMetrics.port = 6800;

        String tpId = "test";
        String compName = "bolt";
        int taskId = 1;
        String streamId = "defaultStream";
        String type = MetaType.STREAM.getV() + MetricType.COUNTER.getV();
        String metricName = "counter1.abcde";
        String group = "udf";

        String name = MetricUtils.metricName(type, tpId, compName, taskId, streamId, group, metricName);
        System.out.println(name);

        AsmCounter counter = new AsmCounter();
        AsmMetric ret1 = JStormMetrics.registerStreamMetric(name, counter, true);
        AsmMetric ret2 = JStormMetrics.registerStreamMetric(name, counter, false);
        System.out.println(ret1 == ret2);

        metricName = MetricUtils.workerMetricName("metric1", MetricType.COUNTER);
        System.out.println(metricName);
        metricName = fixNameIfPossible(metricName);
        System.out.println(metricName);
        System.out.println(fixNameIfPossible(metricName));
    }

}
