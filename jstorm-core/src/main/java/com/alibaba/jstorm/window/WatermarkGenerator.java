package com.alibaba.jstorm.window;

import backtype.storm.task.TopologyContext;
import java.io.Serializable;
import java.util.Map;

/**
 * @author wange
 * @since 16/12/16
 */
public interface WatermarkGenerator extends Serializable {
    void init(Map conf, TopologyContext context);

    long getCurrentWatermark();

    void onElement(long timestamp);

    long getWatermarkInterval();
}
