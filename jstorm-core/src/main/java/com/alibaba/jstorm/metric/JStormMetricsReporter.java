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

import backtype.storm.Config;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.generated.WorkerUploadMetrics;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable.Update;
import com.alibaba.jstorm.daemon.supervisor.SupervisorManger;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.task.execute.BoltCollector;
import com.alibaba.jstorm.task.execute.spout.SpoutCollector;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * report metrics from worker to nimbus server. this class serves as an object in Worker/Nimbus/Supervisor.
 * when in Worker, it reports data via netty transport; otherwise reports via thrift.
 * <p/>
 * there are 2 threads:
 * 1.flush thread: check every 1 sec, when current time is aligned to 1 min, flush all metrics to snapshots
 * 2.check meta thread: use thrift to get metric id from nimbus server.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class JStormMetricsReporter {
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

    private boolean isInWorker = false;

    private SpoutOutputCollector spoutOutput;
    private OutputCollector boltOutput;

    private NimbusClient client = null;

    public JStormMetricsReporter(Object role) {
        LOG.info("starting jstorm metrics reporter in {}", role.getClass().getSimpleName());
        if (role instanceof WorkerData) {
            WorkerData workerData = (WorkerData) role;
            this.conf = workerData.getStormConf();
            this.topologyId = (String) conf.get(Config.TOPOLOGY_ID);
            this.port = workerData.getPort();
            this.isInWorker = true;
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
        JStormMetrics.enabled = ConfigExtension.isEnableMetrics(conf);
        JStormMetrics.setDebug(ConfigExtension.isEnableMetricDebug(conf));
        JStormMetrics.addDebugMetrics(ConfigExtension.getDebugMetricNames(conf));
        JStormMetrics.setTimerUpdateInterval(ConfigExtension.getTimerUpdateInterval(conf));

        this.host = JStormMetrics.getHost();
        if (!JStormMetrics.enabled) {
            LOG.warn("***** topology metrics is disabled! *****");
        } else {
            LOG.info("topology metrics is enabled.");
        }

        this.checkMetaThreadCycle = 30;
        // flush metric snapshots when time is aligned, check every sec.
        this.flushMetricThreadCycle = 1;

        LOG.info("check meta thread freq: {} sec, flush metrics thread freq: {} sec",
                checkMetaThreadCycle, flushMetricThreadCycle);

        this.localMode = StormConfig.local_mode(conf);
        this.clusterName = ConfigExtension.getClusterName(conf);
        LOG.info("done.");
    }

    @VisibleForTesting
    JStormMetricsReporter() {
        LOG.info("Successfully started jstorm metrics reporter for test.");
        this.test = true;
        this.flushMetricThreadCycle = 1;
        this.checkMetaThreadCycle = 30;
    }

    public void init() {
        if (!localMode && JStormMetrics.enabled) {
            this.checkMetricMetaThread = new AsyncLoopThread(new CheckMetricMetaThread());
            this.flushMetricThread = new AsyncLoopThread(new FlushMetricThread());
        }
    }

    private Map<String, Long> registerMetrics(Set<String> names) {
        if (test || !JStormMetrics.enabled) {
            return new HashMap<>();
        }
        try {
            if (client == null) {
                client = NimbusClient.getConfiguredClient(conf);
            }

            return client.getClient().registerMetrics(topologyId, names);
        } catch (Exception e) {
            LOG.error("Failed to gen metric ids", e);
            if (client != null) {
                client.close();
                client = NimbusClient.getConfiguredClient(conf);
            }
        }

        return null;
    }

    public void shutdown() {
        if (!localMode && JStormMetrics.enabled) {
            this.checkMetricMetaThread.cleanup();
            this.flushMetricThread.cleanup();
        }
    }

    public void doUpload() {
        if (test) {
            return;
        }
        try {
            long start = System.currentTimeMillis();
            MetricInfo workerMetricInfo = JStormMetrics.computeAllMetrics();

            WorkerUploadMetrics upload = new WorkerUploadMetrics();
            upload.set_topologyId(topologyId);
            upload.set_supervisorId(host);
            upload.set_port(port);
            upload.set_allMetrics(workerMetricInfo);

            if (workerMetricInfo.get_metrics_size() > 0) {
                uploadMetric(upload);
                LOG.info("Successfully upload worker metrics, size:{}, cost:{}",
                        workerMetricInfo.get_metrics_size(), System.currentTimeMillis() - start);
            } else {
                LOG.info("No metrics to upload.");
            }
        } catch (Exception e) {
            LOG.error("Failed to upload worker metrics", e);
        }
    }


    public void uploadMetric(WorkerUploadMetrics metrics) {
        if (isInWorker) {
            //in Worker, we upload data via netty transport
            if (boltOutput != null) {
                LOG.info("emit metrics through bolt collector.");
                ((BoltCollector)boltOutput.getDelegate()).emitCtrl(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID, null,
                        new Values(JStormServerUtils.getName(host, port), metrics));
/*                boltOutput.emit(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID,
                        new Values(JStormServerUtils.getName(host, port), metrics));*/
            } else if (spoutOutput != null) {
                LOG.info("emit metrics through spout collector.");
                ((SpoutCollector)spoutOutput.getDelegate()).emitCtrl(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID,
                        new Values(JStormServerUtils.getName(host, port), metrics), null);
/*                spoutOutput.emit(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID,
                        new Values(JStormServerUtils.getName(host, port), metrics));*/
            }
        } else {
            // in supervisor or nimbus, we upload metric data via thrift
            LOG.info("emit metrics through nimbus client.");
            Update event = new Update();
            TopologyMetric tpMetric = MetricUtils.mkTopologyMetric();
            tpMetric.set_workerMetric(metrics.get_allMetrics());

            event.topologyMetrics = tpMetric;
            event.topologyId = topologyId;

            try {
                if (client == null) {
                    client = NimbusClient.getConfiguredClient(conf);
                }
                client.getClient().uploadTopologyMetrics(topologyId, tpMetric);
            } catch (Exception ex) {
                LOG.error("upload metric error:", ex);
                if (client != null) {
                    client.close();
                    client = NimbusClient.getConfiguredClient(conf);
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
                    String metricName = metric.getMetricName();
                    if (enabledMetricSet.contains(metricName)) {
                        metric.setEnabled(true);
                    } else if (disabledMetricsSet.contains(metricName)) {
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
            if (enableMetrics) {
                init();
            } else {
                shutdown();
            }
            LOG.info("switch topology metric enable to {}", enableMetrics);
        }
    }

    private Set<String> toSet(String items, String delim) {
        Set<String> ret = new HashSet<>();
        if (!StringUtils.isBlank(items)) {
            String[] metrics = items.split(delim);
            for (String metric : metrics) {
                if (!StringUtils.isBlank(metric)) {
                    ret.add(metric);
                }
            }
        }
        return ret;
    }


    class FlushMetricThread extends RunnableCallback {
        @Override
        public void run() {
            if (TimeUtils.isTimeAligned()) {
                int cnt = 0;
                try {
                    for (AsmMetricRegistry registry : JStormMetrics.allRegistries) {
                        for (Map.Entry<String, AsmMetric> entry : registry.getMetrics().entrySet()) {
                            entry.getValue().flush();
                            cnt++;
                        }
                    }
                    LOG.debug("flush metrics, total:{}.", cnt);

                    doUpload();
                } catch (Exception ex) {
                    LOG.error("Error", ex);
                }
            }
        }

        @Override
        public Object getResult() {
            return flushMetricThreadCycle;
        }
    }

    class CheckMetricMetaThread extends RunnableCallback {
        private volatile boolean processing = false;
        private final long start = TimeUtils.current_time_secs();
        private final long initialDelay = 30 + new Random().nextInt(15);

        @Override
        public void run() {
            if (TimeUtils.current_time_secs() - start < initialDelay) {
                return;
            }

            if (processing) {
                LOG.debug("still processing, skip...");
            } else {
                processing = true;
                long start = System.currentTimeMillis();
                try {
                    Set<String> names = new HashSet<>();
                    for (AsmMetricRegistry registry : JStormMetrics.allRegistries) {
                        Map<String, AsmMetric> metricMap = registry.getMetrics();
                        for (Map.Entry<String, AsmMetric> metricEntry : metricMap.entrySet()) {
                            AsmMetric metric = metricEntry.getValue();
                            if (((metric.getOp() & AsmMetric.MetricOp.REPORT) == AsmMetric.MetricOp.REPORT) &&
                                    metric.getMetricId() == 0L) {
                                names.add(metricEntry.getKey());
                            }
                        }
                    }

                    if (names.size() > 0) {
                        Map<String, Long> nameIdMap = registerMetrics(names);
                        if (nameIdMap != null) {
                            for (String name : nameIdMap.keySet()) {
                                AsmMetric metric = JStormMetrics.find(name);
                                if (metric != null) {
                                    long id = nameIdMap.get(name);
                                    metric.setMetricId(id);
                                    LOG.info("set metric id, {}:{}", name, id);
                                }
                            }
                        }
                        LOG.debug("register metrics, size:{}, cost:{}", names.size(), System.currentTimeMillis() - start);
                    }
                } catch (Exception ex) {
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
}
