package com.alibaba.jstorm.daemon.nimbus.metric;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteMetricEvent extends MetricEvent {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private List<String> idList;
    private int metaType;

    @Override
    public void run() {
        try {
            context.deleteMetric(topologyId, metaType, idList);
        } catch (Exception ex) {
            LOG.error("Error", ex);
        }
    }

    public static void pushEvent(String topologyId, int metaType, List<String> idList) {
        DeleteMetricEvent event = new DeleteMetricEvent();
        event.topologyId = topologyId;
        event.metaType = metaType;
        event.idList = idList;

        ClusterMetricsRunnable.pushEvent(event);
    }
}
