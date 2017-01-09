package com.alibaba.jstorm.daemon.nimbus.metric.merge;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.update.UpdateEvent;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;

public class MergeEvent extends MetricEvent{
    private static final Logger LOG = LoggerFactory.getLogger(MergeEvent.class);

    @Override
    public void run() {
        int secOffset = TimeUtils.secOffset();
        int offset = 55;
        if (secOffset < offset) {
            JStormUtils.sleepMs((offset - secOffset) * 1000);
        } else if (secOffset == offset) {
            // do nothing
        } else {
            JStormUtils.sleepMs((60 - secOffset + offset) * 1000);
        }

        LOG.debug("cluster metrics force upload.");
        mergeAndUploadClusterMetrics();
    }
    
    
    private void mergeAndUploadClusterMetrics() {
        TopologyMetricContext clusterContext = context.getClusterTopologyMetricContext();
        TopologyMetric tpMetric = clusterContext.mergeMetrics();
        if (tpMetric == null) {
            tpMetric = MetricUtils.mkTopologyMetric();
            tpMetric.set_topologyMetric(MetricUtils.mkMetricInfo());
        }

        //reset snapshots metric id
        MetricInfo clusterMetrics = tpMetric.get_topologyMetric();
        Map<String, Long> metricName2Id = clusterContext.getMemMeta();
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : clusterMetrics.get_metrics().entrySet()) {
            String metricName = entry.getKey();
            MetricType metricType = MetricUtils.metricType(metricName);
            Long metricId = metricName2Id.get(metricName);
            for (Map.Entry<Integer, MetricSnapshot> metric : entry.getValue().entrySet()) {
                MetricSnapshot snapshot = metric.getValue();
                snapshot.set_metricId(metricId);
                if (metricType == MetricType.HISTOGRAM) {
                    snapshot.set_points(new byte[0]);
                }
//                entry.getValue().put(metric.getKey(), snapshot);
            }
        }

        //fill the unacquired metrics with zero
        long ts = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : metricName2Id.entrySet()) {
            String name = entry.getKey();
            if (!clusterMetrics.get_metrics().containsKey(name)) {
                Map<Integer, MetricSnapshot> metric = new HashMap<>();
                MetricType type = MetricUtils.metricType(name);
                metric.put(AsmWindow.M1_WINDOW, new MetricSnapshot(entry.getValue(), ts, type.getT()));
                clusterMetrics.put_to_metrics(name, metric);
            }
        }

        //upload to cache
        UpdateEvent.pushEvent(JStormMetrics.CLUSTER_METRIC_KEY, tpMetric);

        LOG.debug("send update event for cluster metrics, size : {}", clusterMetrics.get_metrics_size());
    }

}
