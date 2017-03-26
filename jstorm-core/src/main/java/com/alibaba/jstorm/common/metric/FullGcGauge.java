package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class FullGcGauge implements Gauge<Double> {

    private static final Logger LOG = LoggerFactory.getLogger(FullGcGauge.class);

    public double lastFGcNum = 0.0;

    @Override
    public Double getValue() {
        double newFGcNum = JStormUtils.getFullGC();
        double delta = newFGcNum - lastFGcNum;
        if (delta < 0){
            LOG.warn("new Fgc {} little than old oldFgc {}", newFGcNum, lastFGcNum);
            delta = 0;
        }
        lastFGcNum = newFGcNum;
        return delta;
    }
}
