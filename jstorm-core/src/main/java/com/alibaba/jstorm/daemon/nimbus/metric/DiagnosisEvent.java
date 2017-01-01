package com.alibaba.jstorm.daemon.nimbus.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagnosisEvent extends MetricEvent {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    @Override
    public void run() {
        if (!context.getNimbusData().isLeader()) {
            return;
        }

        // if metricUploader is not fully initialized, return directly
        if (context.getMetricUploader() == null) {
            LOG.info("Context Metric Uploader isn't ready");
            return;
        }

        context.printDiagnosticStats();
    }
}
