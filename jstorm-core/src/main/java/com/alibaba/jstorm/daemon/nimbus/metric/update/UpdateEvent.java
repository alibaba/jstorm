package com.alibaba.jstorm.daemon.nimbus.metric.update;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsContext;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.TopologyMetricDataInfo;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.TopologyMetricContext;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;


public class UpdateEvent extends MetricEvent {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateEvent.class);

    private TopologyMetric topologyMetrics;

    /**
     * put metric data to metric cache.
     */
    @Override
    public void run() {
        if (!context.getTopologyMetricContexts().containsKey(topologyId)) {
            LOG.warn("topology {} has been killed or has not started, skip update.", topologyId);
            return;
        }

        // double check and reset stream metrics if disabled
        if (!JStormMetrics.enableStreamMetrics) {
            topologyMetrics.set_streamMetric(new MetricInfo());
        }

        if (!JStormMetrics.CLUSTER_METRIC_KEY.equals(topologyId)) {
            updateClusterMetrics(topologyId, topologyMetrics);
        }

        // overwrite nimbus-local metrics data
        context.getMetricCache().putMetricData(topologyId, topologyMetrics);

        // below process is kind of a transaction, first we lock an empty slot, mark it as PRE_SET
        // by this time the slot is not yet ready for uploading as the upload thread looks for SET slots only
        // after all metrics data has been saved, we mark it as SET, then it's ready for uploading.
        int idx = context.getAndPresetFirstEmptyIndex();
        if (idx < 0) {
            LOG.error("Exceeding maxPendingUploadMetrics(too much metrics in local rocksdb), " +
                    "skip caching metrics data for topology:{}", topologyId);
            return;
        }

        TopologyMetricDataInfo summary = new TopologyMetricDataInfo();
        int total = 0;
        summary.topologyId = topologyId;
        summary.timestamp = timestamp;
        if (topologyId.equals(JStormMetrics.NIMBUS_METRIC_KEY) || topologyId.equals(JStormMetrics.CLUSTER_METRIC_KEY)) {
            summary.type = MetricUploader.METRIC_TYPE_TOPLOGY;
        } else {
            total += topologyMetrics.get_topologyMetric().get_metrics_size()
                    + topologyMetrics.get_componentMetric().get_metrics_size();
            if (total > 0) {
                int sub = topologyMetrics.get_taskMetric().get_metrics_size()
                        + topologyMetrics.get_workerMetric().get_metrics_size()
                        + topologyMetrics.get_nettyMetric().get_metrics_size()
                        + topologyMetrics.get_streamMetric().get_metrics_size();
                if (sub > 0) {
                    total += sub;
                    summary.type = MetricUploader.METRIC_TYPE_ALL;
                } else {
                    summary.type = MetricUploader.METRIC_TYPE_TOPLOGY;
                }
            } else {
                summary.type = MetricUploader.METRIC_TYPE_TASK;
                total += topologyMetrics.get_taskMetric().get_metrics_size();
            }
        }

        context.getMetricCache().put(ClusterMetricsContext.PENDING_UPLOAD_METRIC_DATA_INFO + idx, summary);
        context.getMetricCache().put(ClusterMetricsContext.PENDING_UPLOAD_METRIC_DATA + idx, topologyMetrics);
        context.markSet(idx);
        LOG.debug("Put metric data to local cache, topology:{}, idx:{}, total:{}", topologyId, idx, total);

    }

    //update cluster metrics local cache
    private void updateClusterMetrics(String topologyId, TopologyMetric tpMetric) {
        if (tpMetric.get_topologyMetric().get_metrics_size() == 0) {
            return;
        }

        MetricInfo topologyMetrics = tpMetric.get_topologyMetric();
        // make a new MetricInfo to save the topologyId's metric
        MetricInfo clusterMetrics = MetricUtils.mkMetricInfo();
        Set<String> metricNames = new HashSet<>();

        for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : topologyMetrics.get_metrics().entrySet()) {
            String metricName = MetricUtils.topo2clusterName(entry.getKey());
            MetricType metricType = MetricUtils.metricType(metricName);
            Map<Integer, MetricSnapshot> winData = new HashMap<>();

            for (Map.Entry<Integer, MetricSnapshot> entryData : entry.getValue().entrySet()) {
                MetricSnapshot snapshot = entryData.getValue().deepCopy();
                winData.put(entryData.getKey(), snapshot);
                if (metricType == MetricType.HISTOGRAM) {
                    // reset topology metric points
                    entryData.getValue().set_points(new byte[0]);
                    entryData.getValue().set_pointSize(0);
                }
            }
            clusterMetrics.put_to_metrics(metricName, winData);
            metricNames.add(metricName);
        }

        // save to local cache, waiting for merging
        TopologyMetricContext clusterTpMetricContext = context.getClusterTopologyMetricContext();
        clusterTpMetricContext.addToMemCache(topologyId, clusterMetrics);
        context.registerMetrics(JStormMetrics.CLUSTER_METRIC_KEY, metricNames);

    }

    public static void pushEvent(String topologyId, TopologyMetric topologyMetrics) {
        UpdateEvent event = new UpdateEvent();
        event.setTopologyId(topologyId);
        event.setTopologyMetrics(topologyMetrics);

        ClusterMetricsRunnable.pushEvent(event);
    }

    public TopologyMetric getTopologyMetrics() {
        return topologyMetrics;
    }

    public void setTopologyMetrics(TopologyMetric topologyMetrics) {
        this.topologyMetrics = topologyMetrics;
    }

}
