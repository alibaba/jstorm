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

import com.alibaba.jstorm.common.metric.codahale.ExponentiallyDecayingReservoir;
import com.alibaba.jstorm.common.metric.codahale.JHistogram;
import com.alibaba.jstorm.common.metric.old.operator.convert.AtomicLongToLong;
import com.alibaba.jstorm.common.metric.snapshot.AsmHistogramSnapshot;
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;

import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * each window has a separate histogram, which is recreated after the window cycle.
 */
public class AsmHistogram extends AsmMetric<JHistogram> {
    private final Map<Integer, JHistogram> histogramMap = new ConcurrentHashMap<>();
    private JHistogram unFlushed = newHistogram();
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private static volatile long updateInterval = 10;

    private volatile long lastUpdateTime = System.currentTimeMillis();

    public AsmHistogram() {
        super();
        for (int win : windowSeconds) {
            histogramMap.put(win, newHistogram());
        }
    }

    public AsmHistogram(TimeUnit timeUnit) {
        this();
        setTimeUnit(timeUnit);
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        if (timeUnit != TimeUnit.MILLISECONDS && timeUnit != TimeUnit.NANOSECONDS) {
            throw new RuntimeException("bad time unit, only ms & ns are supported!");
        }
        this.timeUnit = timeUnit;
    }

    public long getTime() {
        if (JStormMetrics.enabled) {
            long now = System.currentTimeMillis();
            long elapsed = now - lastUpdateTime;
            if (elapsed >= updateInterval) {
                lastUpdateTime = now;
                if (timeUnit == TimeUnit.MILLISECONDS) {
                    return now;
                } else {
                    return System.nanoTime();
                }
            }
        }
        return -1L;
    }

    public boolean okToUpdate(long now) {
        return (now - lastUpdateTime >= updateInterval);
    }

    @Override
    public void update(Number obj) {
        if (JStormMetrics.enabled && sample() && enabled.get()) {
            this.unFlushed.update(obj.longValue());
        }
    }

    @Override
    public void updateTime(long start) {
        if (JStormMetrics.enabled) {
            if (sample() && enabled.get() && start >= 0L) {
                if (timeUnit == TimeUnit.MILLISECONDS) {
                    long end = System.currentTimeMillis();
                    this.unFlushed.update((end - start) * TimeUtils.US_PER_MS);
                } else {
                    long end = System.nanoTime();
                    this.unFlushed.update((end - start) / TimeUtils.NS_PER_US);
                }
            }
        }
    }

    public void updateTime(long start, int n) {
        if (JStormMetrics.enabled) {
            if (sample() && enabled.get() && start >= 0L) {
                if (timeUnit == TimeUnit.MILLISECONDS) {
                    long end = System.currentTimeMillis();
                    long delta = (end - start) / n;
                    this.unFlushed.update(delta * TimeUtils.US_PER_MS);
                } else {
                    long end = System.nanoTime();
                    long delta = (end - start) / n;
                    this.unFlushed.update(delta / TimeUtils.NS_PER_US);
                }
            }
        }
    }

    public void setLastUpdateTime(long time) {
        lastUpdateTime = time;
    }

    @Override
    public void updateDirectly(Number obj) {
        this.unFlushed.update(obj.longValue());
    }

    @Override
    public Map<Integer, JHistogram> getWindowMetricMap() {
        return histogramMap;
    }

    @Override
    public JHistogram mkInstance() {
        return newHistogram();
    }

    @Override
    protected void updateSnapshot(int window) {
        JHistogram histogram = histogramMap.get(window);
        if (histogram != null) {
            AsmSnapshot snapshot = new AsmHistogramSnapshot().setSnapshot(histogram.getSnapshot())
                    .setTs(System.currentTimeMillis()).setMetricId(metricId);
            snapshots.put(window, snapshot);
        }
    }

    /**
     * flush temp histogram data to all windows & assoc metrics.
     */
    protected void doFlush() {
        long[] values = unFlushed.getSnapshot().getValues();
        for (JHistogram histogram : histogramMap.values()) {
            for (long val : values) {
                histogram.update(val);
            }
        }
        if (MetricUtils.metricAccurateCal){
            for (long val : values) {
                for (AsmMetric metric : this.assocMetrics) {
                    metric.updateDirectly(val);
                }
            }
        }
        this.unFlushed = newHistogram();
    }

    @Override
    public AsmMetric clone() {
        return new AsmHistogram(timeUnit);
    }

    private JHistogram newHistogram() {
        return new JHistogram(new ExponentiallyDecayingReservoir());
    }

    public static void setUpdateInterval(long interval) {
        if (interval < 0 || interval > TimeUtils.MS_PER_SEC) {
            throw new RuntimeException("timerUpdateInterval must be between 0~1000");
        }
        updateInterval = interval;
    }

    public static long getUpdateInterval() {
        return updateInterval;
    }
}
