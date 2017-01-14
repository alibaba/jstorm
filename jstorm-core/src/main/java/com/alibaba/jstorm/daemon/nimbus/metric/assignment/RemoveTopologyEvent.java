package com.alibaba.jstorm.daemon.nimbus.metric.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;

public class RemoveTopologyEvent extends MetricEvent {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveTopologyEvent.class);
    
    public void removeTopology(String topologyId) {
        context.getMetricCache().removeTopology(topologyId);
        context.getMetricCache().removeSampleRate(topologyId);
        
        context.getTopologyMetricContexts().remove(topologyId);
    }
    
    @Override
    public void run() {
        if (topologyId != null) {
            removeTopology(topologyId);
        }
        LOG.info("remove topology:{}.", topologyId);
    }
    
    public static void pushEvent(String topologyId) {
        RemoveTopologyEvent event = new RemoveTopologyEvent();
        event.setTopologyId(topologyId);
        event.setClusterMetricsContext(ClusterMetricsRunnable.getInstance().getContext());
        
//        ClusterMetricsRunnable.pushEvent(event);
        
        // directly remove event, skip issue remove event
        event.run();
        
    }
}
