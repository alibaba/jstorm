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

import com.alibaba.jstorm.common.metric.snapshot.AsmMeterSnapshot;
import com.alibaba.jstorm.metric.MetricUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * one meter & one snapshot for all windows. since meter is window-sliding, there's no need to recreate new ones.
 */
public class AsmMeter extends AsmMetric<MeanMeter> {
    private final AtomicLong unflushed = new AtomicLong(0l);

    private final Map<Integer, MeanMeter> meterMap = new ConcurrentHashMap<>();

    public AsmMeter() {
        super();
        for (int win : windowSeconds) {
            meterMap.put(win, new MeanMeter());
        }
    }

    public void mark() {
        unflushed.addAndGet(1L);
    }

    @Override
    public void update(Number obj) {
        if (enabled.get()) {
            unflushed.addAndGet(obj.longValue());
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
    public Map<Integer, MeanMeter> getWindowMetricMap() {
        return meterMap;
    }

    @Override
    protected void doFlush() {
        long v;
        synchronized (unflushed) {
            v = unflushed.get();
        }
        for (MeanMeter meter : meterMap.values()) {
            meter.mark(v);
        }
        this.unflushed.addAndGet(-v);

        if (MetricUtils.metricAccurateCal) {
            for (AsmMetric assocMetric : assocMetrics) {
                assocMetric.updateDirectly(v);
            }
        }
    }

    @Override
    protected void updateSnapshot(int window) {
        AsmMeterSnapshot meterSnapshot = new AsmMeterSnapshot();
        MeanMeter meter = meterMap.get(window);
        if (meter != null) {
            meterSnapshot.setM1(meter.getOneMinuteRate()).setM5(meter.getFiveMinuteRate())
                    .setM15(meter.getFifteenMinuteRate()).setMean(meter.getMeanRate())
                    .setTs(System.currentTimeMillis()).setMetricId(metricId);
            snapshots.put(window, meterSnapshot);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(Integer window) {
        synchronized (this) {
            long v = unflushed.get();
            for (MeanMeter meter : meterMap.values()) {
                meter.mark(v);
            }
            unflushed.addAndGet(-v);

            return meterMap.get(window).getOneMinuteRate();
        }
    }

    @Override
    public MeanMeter mkInstance() {
        return new MeanMeter();
    }
}
