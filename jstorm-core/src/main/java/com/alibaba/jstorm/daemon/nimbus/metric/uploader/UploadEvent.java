package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.utils.JStormUtils;

public class UploadEvent extends MetricEvent {
    private static final Logger LOG = LoggerFactory.getLogger(UploadEvent.class);

    @Override
    public void run() {
        AtomicBoolean isShutdown = context.getNimbusData().getIsShutdown();
        while (isShutdown != null && !isShutdown.get()) {
            if (!context.getNimbusData().isLeader() || !context.isReadyToUpload()) {
                JStormUtils.sleepMs(10);
                continue;
            }
            try {
                final int idx = context.getFirstPendingUploadIndex();
                if (idx >= 0) {
                    if (!context.getMetricUploaderDelegate().isUnderFlowControl()) {
                        context.markUploading(idx);
                        upload(idx);
                    }
                } else {
                    JStormUtils.sleepMs(5);
                }
            } catch (Exception ex) {
                LOG.error("Error", ex);
            }
        }
    }

    public boolean upload(final int idx) {
        final String clusterName = context.getClusterName();
        final TopologyMetricDataInfo summary = context.getMetricDataInfoFromCache(idx);

        if (summary == null) {
            LOG.warn("metric summary is null from cache idx:{}", idx);
            context.forceMarkUploaded(idx);
            return true;
        }

        final String topologyId = summary.topologyId;
        if (!context.isTopologyAlive(topologyId)) {
            LOG.warn("topology {} is not alive, skip sending metrics.", topologyId);
            context.forceMarkUploaded(idx);
            return true;
        }

        // NOTE that metric uploader must be sync so it can safely decrement uploading num
        // otherwise the uploading num will be inaccurate
        return context.getMetricUploaderDelegate().upload(clusterName, topologyId, idx, summary.toMap());
        //context.markUploaded(idx);
    }

}
