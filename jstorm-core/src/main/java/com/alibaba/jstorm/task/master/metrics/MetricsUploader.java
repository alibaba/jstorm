package com.alibaba.jstorm.task.master.metrics;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMasterContext;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Maps;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.NimbusClientWrapper;

public class MetricsUploader implements TMHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsUploader.class);
    static private final Logger metricLogger = TopologyMetricContext.LOG;

    public static final int MAX_BATCH_SIZE = 10000;
    private static final int UPLOAD_TIME_OFFSET_SEC = 35;

    private final MetricInfo dummy = MetricUtils.mkMetricInfo();
    private TopologyMetricContext topologyMetricContext;
    private Map conf;
    private String topologyId;
    private NimbusClientWrapper client;

    private StormClusterState zkCluster;
    private TopologyContext context;
    private final Object lock = new Object();

    private final AtomicBoolean uploading = new AtomicBoolean(false);

    @Override
    public void init(TopologyMasterContext tmContext) {
        this.context = tmContext.getContext();
        this.topologyMetricContext = tmContext.getTopologyMetricContext();
        this.conf = tmContext.getConf();
        this.topologyId = tmContext.getTopologyId();
        this.zkCluster = tmContext.getZkCluster();
    }

    /**
     * Wait UPLOAD_TIME_OFFSET_SEC sec to ensure we've collected enough metrics from
     * topology workers, note that it's not guaranteed metrics from all workers will be collected.
     *
     * If we miss the offset, we'll sleep until the offset comes next minute.
     */
    @Override
    public void process(Object event) throws Exception {
        int secOffset = TimeUtils.secOffset();
        if (secOffset < UPLOAD_TIME_OFFSET_SEC) {
            JStormUtils.sleepMs((UPLOAD_TIME_OFFSET_SEC - secOffset) * 1000);
        } else if (secOffset == UPLOAD_TIME_OFFSET_SEC) {
            // do nothing
        } else {
            JStormUtils.sleepMs((60 - secOffset + UPLOAD_TIME_OFFSET_SEC) * 1000);
        }
        if (topologyMetricContext.getUploadedWorkerNum() > 0) {
            metricLogger.info("force upload metrics.");
            mergeAndUploadMetrics();
        }
    }

    @Override
    public void cleanup() {
    }

    private void mergeAndUploadMetrics() throws Exception {
        if (uploading.compareAndSet(false, true)) {
            // double check
            if (topologyMetricContext.getUploadedWorkerNum() > 0) {
                TopologyMetric tpMetric = topologyMetricContext.mergeMetrics();
                if (tpMetric != null) {
                    uploadMetrics(tpMetric);
                }
            }
            uploading.set(false);
        } else {
            LOG.warn("another thread is already uploading, skip...");
        }
    }

    /**
     * upload metrics sequentially due to thrift frame size limit (15MB)
     */
    private void uploadMetrics(TopologyMetric tpMetric) throws Exception {
        long start = System.currentTimeMillis();

        if (tpMetric == null) {
            return;
        }
        try {
            synchronized (lock) {
                if (client == null || !client.isValid()) {
                    client = new NimbusClientWrapper();
                    client.init(conf);
                }
            }

            MetricInfo topologyMetrics = tpMetric.get_topologyMetric();
            MetricInfo componentMetrics = tpMetric.get_componentMetric();
            MetricInfo taskMetrics = tpMetric.get_taskMetric();
            MetricInfo streamMetrics = tpMetric.get_streamMetric();
            MetricInfo workerMetrics = tpMetric.get_workerMetric();
            MetricInfo nettyMetrics = tpMetric.get_nettyMetric();

            int totalSize = topologyMetrics.get_metrics_size()
                    + componentMetrics.get_metrics_size()
                    + taskMetrics.get_metrics_size()
                    + streamMetrics.get_metrics_size()
                    + workerMetrics.get_metrics_size()
                    + nettyMetrics.get_metrics_size();

            // for small topologies, send all metrics together to ease the
            // pressure of nimbus
            if (totalSize < MAX_BATCH_SIZE) {
                client.getClient().uploadTopologyMetrics(topologyId,
                        new TopologyMetric(topologyMetrics, componentMetrics,
                                workerMetrics, taskMetrics, streamMetrics,
                                nettyMetrics));
            } else {
                client.getClient().uploadTopologyMetrics(topologyId,
                        new TopologyMetric(topologyMetrics, componentMetrics,
                                dummy, dummy, dummy, dummy));
                batchUploadMetrics(workerMetrics, MetaType.WORKER);
                batchUploadMetrics(taskMetrics, MetaType.TASK);
                batchUploadMetrics(streamMetrics, MetaType.STREAM);
                batchUploadMetrics(nettyMetrics, MetaType.NETTY);
            }
        } catch (Exception e) {
            String errorInfo = "Failed to upload worker metrics";
            LOG.error("Failed to upload worker metrics ", e);
            if (client != null) {
                client.cleanup();
            }
            zkCluster.report_task_error(
                    context.getTopologyId(), context.getThisTaskId(), errorInfo, ErrorConstants.WARN, ErrorConstants.CODE_USER);
        }

        metricLogger.info("upload metrics, cost:{}", System.currentTimeMillis() - start);
    }

    private void batchUploadMetrics(MetricInfo metricInfo, MetaType metaType)
            throws Exception {
        if (metricInfo.get_metrics_size() > MAX_BATCH_SIZE) {
            Map<String, Map<Integer, MetricSnapshot>> data = metricInfo.get_metrics();

            Map<String, Map<Integer, MetricSnapshot>> part = Maps
                    .newHashMapWithExpectedSize(MAX_BATCH_SIZE);
            MetricInfo uploadPart = new MetricInfo();
            int i = 0;
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : data.entrySet()) {
                part.put(entry.getKey(), entry.getValue());
                if (++i >= MAX_BATCH_SIZE) {
                    uploadPart.set_metrics(part);
                    uploadParts(uploadPart, metaType);

                    i = 0;
                    part.clear();
                }
            }
            if (part.size() > 0) {
                uploadPart.set_metrics(part);
                uploadParts(uploadPart, metaType);
            }
        } else {
            uploadParts(metricInfo, metaType);
        }
    }

    private void uploadParts(MetricInfo part, MetaType metaType) throws Exception {
        if (metaType == MetaType.TASK) {
            client.getClient().uploadTopologyMetrics(topologyId,
                    new TopologyMetric(dummy, dummy, dummy, part, dummy, dummy));
        } else if (metaType == MetaType.STREAM) {
            client.getClient().uploadTopologyMetrics(topologyId,
                    new TopologyMetric(dummy, dummy, dummy, dummy, part, dummy));
        } else if (metaType == MetaType.WORKER) {
            client.getClient().uploadTopologyMetrics(topologyId,
                    new TopologyMetric(dummy, dummy, part, dummy, dummy, dummy));
        } else if (metaType == MetaType.NETTY) {
            client.getClient().uploadTopologyMetrics(topologyId,
                    new TopologyMetric(dummy, dummy, dummy, dummy, dummy, part));
        }
    }

}
