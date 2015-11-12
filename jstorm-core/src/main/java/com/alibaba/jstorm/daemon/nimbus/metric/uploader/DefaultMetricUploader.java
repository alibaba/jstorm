package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultMetricUploader implements MetricUploader {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected NimbusData nimbusData;

    public DefaultMetricUploader() {
    }

    @Override
    public void init(NimbusData nimbusData) throws Exception {
        this.nimbusData = nimbusData;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public boolean registerMetrics(String clusterName, String topologyId,
                                   Map<String, Long> metrics) {
        if (metrics.size() > 0) {
            logger.info("register metrics, topology:{}, total:{}", topologyId, metrics.size());
        }
        return true;
    }

    @Override
    public boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric, Map<String, Object> metricContext) {
        if (tpMetric == null) {
            logger.info("No metric of {}", topologyId);
            return true;
        }

        logger.info("send metrics info, topology:{}, metric:{}, metricContext:{}", topologyId,
                Utils.toPrettyJsonString(tpMetric), metricContext);
        return true;
    }

    @Override
    public boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext) {
        return true;
    }


    @Override
    public boolean sendEvent(String clusterName, Event event) {
        logger.info("Successfully sendEvent {} of {}", event, clusterName);
        return true;
    }
}