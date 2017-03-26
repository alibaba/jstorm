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

import com.alibaba.jstorm.config.Refreshable;
import com.alibaba.jstorm.config.RefreshableComponents;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.supervisor.SupervisorManger;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.task.execute.BoltCollector;
import com.alibaba.jstorm.task.execute.spout.SpoutCollector;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.annotations.VisibleForTesting;

import backtype.storm.Config;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.generated.WorkerUploadMetrics;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClientWrapper;

/**
 * report metrics from worker to nimbus server. this class serves as an object in Worker/Nimbus/Supervisor.
 * when in a Worker, it reports data via netty transport(to topology master first)
 * otherwise reports via thrift to nimbus directly
 * <p/>
 * there are 2 threads:
 * 1.flush thread: check every 1 sec, when current time is aligned to 1 min, flush all metrics to snapshots
 * 2.check meta thread: use thrift to get metric id from nimbus server.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class JStormMetricsReporter implements Refreshable {
    private static final Logger LOG = LoggerFactory.getLogger(JStormMetricsReporter.class);

    private Map conf;
    protected String clusterName;
    protected String topologyId;
    protected String host;
    protected int port;

    protected boolean localMode = false;

    private AsyncLoopThread checkMetricMetaThread;
    protected final int checkMetaThreadCycle;

    private AsyncLoopThread flushMetricThread;
    protected final int flushMetricThreadCycle;

    private boolean test = false;

    private boolean inTopology = false;

    private volatile SpoutOutputCollector spoutOutput;
    private volatile OutputCollector boltOutput;

    private NimbusClientWrapper client = null;
    private MetricsRegister metricsRegister;

    public JStormMetricsReporter(Object role) {
        LOG.info("starting jstorm metrics reporter in {}", role.getClass().getSimpleName());
        if (role instanceof WorkerData) {
            WorkerData workerData = (WorkerData) role;
            this.conf = workerData.getStormConf();
            this.topologyId = (String) conf.get(Config.TOPOLOGY_ID);
            this.port = workerData.getPort();
            this.inTopology = true;
        } else if (role instanceof NimbusData) {
            NimbusData nimbusData = (NimbusData) role;
            this.conf = nimbusData.getConf();
            this.topologyId = JStormMetrics.NIMBUS_METRIC_KEY;
        } else if (role instanceof SupervisorManger) {
            SupervisorManger supervisor = (SupervisorManger) role;
            this.conf = supervisor.getConf();
            this.topologyId = JStormMetrics.SUPERVISOR_METRIC_KEY;
            JStormMetrics.setTopologyId(this.topologyId);
        }

        // update metrics config
        refresh(conf);

        this.metricsRegister = new MetricsRegister(conf, topologyId);
        this.host = JStormMetrics.getHost();
        if (!JStormMetrics.enabled) {
            LOG.warn("***** topology metrics is disabled! *****");
        } else {
            LOG.info("topology metrics is enabled.");
        }

        this.checkMetaThreadCycle = 20;
        // flush metric snapshots when time is aligned, check every sec.
        this.flushMetricThreadCycle = 1;

        LOG.info("check meta thread freq: {} sec, flush metrics thread freq: {} sec",
                checkMetaThreadCycle, flushMetricThreadCycle);

        this.localMode = StormConfig.local_mode(conf);
        this.clusterName = ConfigExtension.getClusterName(conf);

        RefreshableComponents.registerRefreshable(this);
        LOG.info("done.");
    }

    @VisibleForTesting
    JStormMetricsReporter() {
        LOG.info("Successfully started jstorm metrics reporter for test.");
        this.test = true;
        this.flushMetricThreadCycle = 1;
        this.checkMetaThreadCycle = 20;
    }

    public void init() {
        if (JStormMetrics.enabled) {
            this.checkMetricMetaThread = new AsyncLoopThread(new CheckMetricMetaThread());
            this.flushMetricThread = new AsyncLoopThread(new FlushMetricThread());
        }
    }

    public void shutdown() {
        if (JStormMetrics.enabled) {
            this.checkMetricMetaThread.cleanup();
            this.flushMetricThread.cleanup();
        }
    }

    private Map<String, Long> registerMetrics(Set<String> names) {
        if (test) {
            return new HashMap<>();
        }
        return metricsRegister.registerMetrics(names);
    }

    public void uploadMetricData() {
        if (test) {
            return;
        }
        try {
            long start = System.currentTimeMillis();
            MetricInfo workerMetricInfo = MetricUtils.metricAccurateCal ?
                    JStormMetrics.computeAllMetrics() : JStormMetrics.approximateComputeAllMetrics();

            WorkerUploadMetrics upload = new WorkerUploadMetrics();
            upload.set_topologyId(topologyId);
            upload.set_supervisorId(host);
            upload.set_port(port);
            upload.set_allMetrics(workerMetricInfo);

            if (workerMetricInfo.get_metrics_size() > 0) {
                uploadMetricData(upload);
                LOG.debug("Successfully upload worker metrics, size:{}, cost:{}",
                        workerMetricInfo.get_metrics_size(), System.currentTimeMillis() - start);
            } else {
                LOG.debug("No metrics to upload.");
            }
        } catch (Exception e) {
            LOG.error("Failed to upload worker metrics", e);
        }
    }

    public void uploadMetricData(WorkerUploadMetrics metrics) {
        if (inTopology) {
            //in Worker, we upload data via netty transport
            if (boltOutput != null) {
                LOG.debug("emit metrics through bolt collector.");
                ((BoltCollector) boltOutput.getDelegate()).emitCtrl(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID, null,
                        new Values(JStormServerUtils.getName(host, port), metrics));
            } else if (spoutOutput != null) {
                LOG.debug("emit metrics through spout collector.");
                ((SpoutCollector) spoutOutput.getDelegate()).emitCtrl(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID,
                        new Values(JStormServerUtils.getName(host, port), metrics), null);
            } else {
                LOG.warn("topology:{}, both spout/bolt collectors are null, don't know what to do...", topologyId);
            }
        } else {
            // in supervisor or nimbus, we upload metric data via thrift
            LOG.debug("emit metrics through nimbus client.");
            TopologyMetric tpMetric = MetricUtils.mkTopologyMetric();
            tpMetric.set_workerMetric(metrics.get_allMetrics());

            //UpdateEvent.pushEvent(topologyId, tpMetric);
            try {
                // push metrics via nimbus client
                if (client == null) {
                    LOG.warn("nimbus client is null...");
                    client = new NimbusClientWrapper();
                    client.init(conf);
                }
                client.getClient().uploadTopologyMetrics(topologyId, tpMetric);
            } catch (Throwable ex) {
                LOG.error("upload metrics error:", ex);
                if (client != null) {
                    client.cleanup();
                    client = null;
                }
            }
        }
        //MetricUtils.logMetrics(metrics.get_allMetrics());
    }

    public void setOutputCollector(Object outputCollector) {
        if (outputCollector instanceof OutputCollector) {
            this.boltOutput = (OutputCollector) outputCollector;
        } else if (outputCollector instanceof SpoutOutputCollector) {
            this.spoutOutput = (SpoutOutputCollector) outputCollector;
        }
    }

    public void updateMetricConfig(Map newConf) {
        JStormMetrics.setDebug(ConfigExtension.isEnableMetricDebug(conf));
        JStormMetrics.addDebugMetrics(ConfigExtension.getDebugMetricNames(conf));
        //update metric accurate calculate
        boolean accurateMetric = ConfigExtension.getTopologyAccurateMetric(newConf);
        if (MetricUtils.metricAccurateCal != accurateMetric) {
            MetricUtils.metricAccurateCal = accurateMetric;
            LOG.info("switch topology metric accurate enable to {}", MetricUtils.metricAccurateCal);
        }

        // update enabled/disabled metrics
        String enabledMetrics = ConfigExtension.getEnabledMetricNames(newConf);
        String disabledMetrics = ConfigExtension.getDisabledMetricNames(newConf);
        if (enabledMetrics != null || disabledMetrics != null) {
            Set<String> enabledMetricSet = toSet(enabledMetrics, ",");
            Set<String> disabledMetricsSet = toSet(disabledMetrics, ",");

            AsmMetricRegistry[] registries = new AsmMetricRegistry[]{
                    JStormMetrics.getTopologyMetrics(),
                    JStormMetrics.getComponentMetrics(),
                    JStormMetrics.getTaskMetrics(),
                    JStormMetrics.getStreamMetrics(),
                    JStormMetrics.getNettyMetrics(),
                    JStormMetrics.getWorkerMetrics()
            };
            for (AsmMetricRegistry registry : registries) {
                Collection<AsmMetric> metrics = registry.getMetrics().values();
                for (AsmMetric metric : metrics) {
                    String shortMetricName = metric.getShortName();
                    if (enabledMetricSet.contains(shortMetricName)) {
                        metric.setEnabled(true);
                    } else if (disabledMetricsSet.contains(shortMetricName)) {
                        metric.setEnabled(false);
                    }
                }
            }
        }

        long updateInterval = ConfigExtension.getTimerUpdateInterval(newConf);
        if (updateInterval != AsmHistogram.getUpdateInterval()) {
            AsmHistogram.setUpdateInterval(updateInterval);
        }

        boolean enableStreamMetrics = ConfigExtension.isEnableStreamMetrics(newConf);
        if (enableStreamMetrics != JStormMetrics.enableStreamMetrics) {
            JStormMetrics.enableStreamMetrics = enableStreamMetrics;
            LOG.info("switch topology stream metric enable to {}", enableStreamMetrics);
        }

        boolean enableMetrics = ConfigExtension.isEnableMetrics(newConf);
        if (enableMetrics != JStormMetrics.enabled) {
            JStormMetrics.enabled = enableMetrics;
            LOG.info("switch topology metric enable to {}", enableMetrics);
        }
    }

    private Set<String> toSet(String items, String delim) {
        Set<String> ret = new HashSet<>();
        if (!StringUtils.isBlank(items)) {
            String[] metrics = items.split(delim);
            for (String metric : metrics) {
                metric = metric.trim();
                if (!StringUtils.isBlank(metric)) {
                    ret.add(metric);
                }
            }
        }
        return ret;
    }

    @Override
    public void refresh(Map conf) {
        updateMetricConfig(conf);
    }


    /**
     * A thread which flushes metrics data on aligned time, and sends metrics data to:
     * 1. nimbus via nimbus client if this JStormMetricsReporter instance is not in a topology worker
     * 2. topology master via netty if it's in a topology worker
     */
    class FlushMetricThread extends RunnableCallback {
        @Override
        public void run() {
            if (!JStormMetrics.enabled || !TimeUtils.isTimeAligned()) {
                return;
            }

            int cnt = 0;
            try {
                for (AsmMetricRegistry registry : JStormMetrics.allRegistries) {
                    for (Map.Entry<String, AsmMetric> entry : registry.getMetrics().entrySet()) {
                        entry.getValue().flush();
                        cnt++;
                    }
                }
                LOG.debug("flush metrics, total:{}.", cnt);

                uploadMetricData();
            } catch (Exception ex) {
                LOG.error("Error", ex);
            }
        }

        @Override
        public Object getResult() {
            return flushMetricThreadCycle;
        }
    }


    /**
     * A thread which checks metric meta every checkMetaThreadCycle seconds, and tries to:
     * 1. register metrics via nimbus client if it's not in a topology worker
     * 2. register metrics to topology master if it's in a topology worker
     */
    class CheckMetricMetaThread extends RunnableCallback {
        private volatile boolean processing = false;
        private final long start = TimeUtils.current_time_secs();
        private final long initialDelay = 15 + new Random().nextInt(15);

        @Override
        public void run() {
            if (!JStormMetrics.enabled || TimeUtils.current_time_secs() - start < initialDelay) {
                return;
            }

            if (processing) {
                LOG.debug("still processing, skip...");
            } else {
                processing = true;
                try {
                    Set<String> names = new HashSet<>();
                    for (AsmMetricRegistry registry : JStormMetrics.allRegistries) {
                        Map<String, AsmMetric> metricMap = registry.getMetrics();
                        for (Map.Entry<String, AsmMetric> metricEntry : metricMap.entrySet()) {
                            AsmMetric metric = metricEntry.getValue();
                            if (((metric.getOp() & AsmMetric.MetricOp.REPORT) == AsmMetric.MetricOp.REPORT) &&
                                    !MetricUtils.isValidId(metric.getMetricId())) {
                                names.add(metricEntry.getKey());
                            }
                        }
                    }

                    // when in nimbus/supervisor, force to check worker metrics(CpuUsedRatio, DiskUsage, etc) again
                    // TODO: ideally, this should only happen in a short period when nimbus restarts lately
                    if (!inTopology) {
                        for (Map.Entry<String, AsmMetric> metricEntry : JStormMetrics.workerMetrics.getMetrics().entrySet()) {
                            AsmMetric metric = metricEntry.getValue();
                            if (((metric.getOp() & AsmMetric.MetricOp.REPORT) == AsmMetric.MetricOp.REPORT)) {
                                names.add(metricEntry.getKey());
                            }
                        }
                    }

                    // register via nimbus client, for supervisors/nimbus servers
                    if (names.size() > 0) {
                        LOG.debug("register metrics, size:{}", names.size());
                        if (!inTopology) {
                            Map<String, Long> nameIdMap = registerMetrics(names);
                            if (nameIdMap != null) {
                                for (String name : nameIdMap.keySet()) {
                                    AsmMetric metric = JStormMetrics.find(name);
                                    if (metric != null) {
                                        long id = nameIdMap.get(name);
                                        metric.setMetricId(id);
                                        LOG.debug("set metric id, {}:{}", name, id);
                                    }
                                }
                            }
                        } else { // register via TM, for topologies
                            if (spoutOutput != null) {
                                ((SpoutCollector) spoutOutput.getDelegate()).emitCtrl(
                                        Common.TOPOLOGY_MASTER_REGISTER_METRICS_STREAM_ID, new Values(names), null);
                            } else if (boltOutput != null) {
                                ((BoltCollector) boltOutput.getDelegate()).emitCtrl(
                                        Common.TOPOLOGY_MASTER_REGISTER_METRICS_STREAM_ID, null, new Values(names));
                            } else {
                                LOG.warn("topology:{}, both spout and bolt collectors are null, don't know what to do...", topologyId);
                            }
                        }
                    }
                } catch (Throwable ex) {
                    LOG.error("Error", ex);
                }
                processing = false;
            }
        }

        @Override
        public Object getResult() {
            return checkMetaThreadCycle;
        }
    }

    /**
     * Register metric meta callback. Called in SpoutExecutors/BoltExecutors within topology workers.
     *
     * JStormMetricsReporter first sends a TOPOLOGY_MASTER_REGISTER_METRICS_STREAM_ID stream to TM to register metrics,
     * on success TM will return a TOPOLOGY_MASTER_REGISTER_METRICS_RESP_STREAM_ID stream which contains
     * registered metric meta and then call this method to update local meta.
     */
    public void updateMetricMeta(Map<String, Long> nameIdMap) {
        if (nameIdMap != null) {
            for (String name : nameIdMap.keySet()) {
                AsmMetric metric = JStormMetrics.find(name);
                if (metric != null) {
                    long id = nameIdMap.get(name);
                    metric.setMetricId(id);
                    LOG.debug("set metric id, {}:{}", name, id);
                }
            }
        }

    }

}
