package com.alibaba.jstorm.metric;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class CallIntervalGauge implements Gauge<Double> {
    private AtomicLong times = new AtomicLong(0);
    private long lastTime = System.currentTimeMillis();
    private Double value = 0.0;

    public long incrementAndGet(){
        return times.incrementAndGet();
    }
    public long addAndGet(long delta){
        return times.addAndGet(delta);
    }


    @Override
    public Double getValue() {
        long totalTimes = times.getAndSet(0);
        long now = System.currentTimeMillis();
        long timeInterval = now - lastTime;
        lastTime = now;
        if (timeInterval > 0 && totalTimes > 0){
            //convert us
            value = timeInterval/(double)totalTimes * 1000;
        }else {
            value = 0.0;
        }
        return value;
    }
}
