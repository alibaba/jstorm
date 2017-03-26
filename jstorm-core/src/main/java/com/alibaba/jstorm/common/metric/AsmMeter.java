/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.common.metric.codahale.JMeter;
import com.alibaba.jstorm.common.metric.snapshot.AsmMeterSnapshot;
import com.alibaba.jstorm.metric.MetricUtils;
import com.codahale.metrics.Meter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * one meter & one snapshot for all windows. since meter is window-sliding, there's no need to recreate new ones.
 */
public class AsmMeter extends AsmMetric<Meter> {
    private static final long UPDATE_INTERVAL_MS = 3000L;
    private final JMeter meter = new JMeter();
    private final AtomicLong unflushed = new AtomicLong(0l);
    private volatile long lastUpdateTime = System.currentTimeMillis();

    public void mark() {
        unflushed.addAndGet(1L);
//        meter.mark(1l);
    }

    @Override
    public void update(Number obj) {
        if (enabled.get()) {
            unflushed.addAndGet(obj.longValue());

            // do flush every updateInterval, make sure meter count rightly
            long now = System.currentTimeMillis();
            long elapsed = now - lastUpdateTime;
            if (elapsed >= UPDATE_INTERVAL_MS) {
                lastUpdateTime = now;
                // here maybe not thread safe, but that's ok
                // because doFlush is thread safe
                doFlush();
            }
//            meter.mark(obj.longValue());
//            for (AsmMetric metric : this.assocMetrics) {
//                metric.update(obj);
//            }
        }
    }

    @Override
    public void updateTime(long obj) {
        throw new RuntimeException("please use update method!");
    }

    @Override
    public AsmMetric clone() {
        return new AsmMeter();
    }

    @Override
    public Map<Integer, Meter> getWindowMetricMap() {
        return null;
    }

    @Override
    protected void doFlush() {
        long v = unflushed.getAndSet(0l);
        meter.mark(v);
        if (MetricUtils.metricAccurateCal){
            for (AsmMetric metric : this.assocMetrics) {
                metric.update(v);
            }
        }
    }

    @Override
    protected void updateSnapshot(int window) {
        AsmMeterSnapshot meterSnapshot = new AsmMeterSnapshot();
        meterSnapshot.setM1(meter.getOneMinuteRate()).setM5(meter.getFiveMinuteRate())
                .setM15(meter.getFifteenMinuteRate()).setMean(meter.getMeanRate())
                .setTs(System.currentTimeMillis()).setMetricId(metricId);
        snapshots.put(window, meterSnapshot);
    }

    @Override
    public Meter mkInstance() {
        return null;
    }
}
