package com.alibaba.jstorm.daemon.nimbus.metric.assignment;

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.metric.TopologyMetricContext;

public class StartTopologyEvent extends MetricEvent {
    private double sampleRate;
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        context.getMetricCache().putSampleRate(topologyId, sampleRate);
        context.getMetricUploader().sendEvent(context.getClusterName(), this);
        
        if (!context.getTopologyMetricContexts().containsKey(topologyId)) {
            TopologyMetricContext metricContext = new TopologyMetricContext();
            // note that workerNum is not set here.
            context.getTopologyMetricContexts().put(topologyId, metricContext);
        }
    }
    
    public double getSampleRate() {
        return sampleRate;
    }
    
    public void setSampleRate(double sampleRate) {
        this.sampleRate = sampleRate;
    }
    
    public static void pushEvent(String topologyId, double sampleRate) {
        StartTopologyEvent startEvent = new StartTopologyEvent();
        startEvent.topologyId = topologyId;
        startEvent.sampleRate = sampleRate;
        ClusterMetricsRunnable.pushEvent(startEvent);
    }
    
}
