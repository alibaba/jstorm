package com.alibaba.jstorm.daemon.nimbus.metric.assignment;

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;

public class KillTopologyEvent extends MetricEvent {
    
    @Override
    public void run() {
        context.getMetricUploaderDelegate().sendEvent(context.getClusterName(), this);
        RemoveTopologyEvent.pushEvent(topologyId);
    }
    
    public static void pushEvent(String topologyId) {
        KillTopologyEvent killEvent = new KillTopologyEvent();
        killEvent.topologyId = topologyId;
        
        ClusterMetricsRunnable.pushEvent(killEvent);
    }
}
