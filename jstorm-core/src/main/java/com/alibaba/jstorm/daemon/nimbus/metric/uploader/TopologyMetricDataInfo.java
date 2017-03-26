package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TopologyMetricDataInfo implements Serializable {
    private static final long serialVersionUID = 1303262512351757610L;
    
    public String topologyId;
    public String type;      // "tp" for tp/comp metrics OR "task" for task/stream/worker/netty metrics
    public long   timestamp; // metrics report time
    
    public Map<String, Object> toMap() {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(MetricUploader.METRIC_TIME, timestamp);
        ret.put(MetricUploader.METRIC_TYPE, type);
        
        return ret;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
