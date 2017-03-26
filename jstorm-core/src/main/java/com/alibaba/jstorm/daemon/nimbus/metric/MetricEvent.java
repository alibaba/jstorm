package com.alibaba.jstorm.daemon.nimbus.metric;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public abstract class MetricEvent implements Runnable {
    
    protected String topologyId;
    
    protected long timestamp = System.currentTimeMillis();
    
    protected ClusterMetricsContext context;
    
    public abstract void run();
    
    public String getTopologyId() {
        return topologyId;
    }
    
    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public ClusterMetricsContext getClusterMetricsContext() {
        return context;
    }
    
    public void setClusterMetricsContext(ClusterMetricsContext clusterMetricsContext) {
        this.context = clusterMetricsContext;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    
}
