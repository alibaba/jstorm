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

import com.alibaba.jstorm.common.metric.snapshot.AsmHistogramSnapshot;
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * each window has a separate histogram, which is recreated after the window cycle.
 */
public class AsmHistogram extends AsmMetric<Histogram> {

    private final Map<Integer, Histogram> histogramMap = new ConcurrentHashMap<Integer, Histogram>();
    private Histogram unFlushed = newHistogram();

    public AsmHistogram() {
        super();
        for (int win : windowSeconds) {
            histogramMap.put(win, newHistogram());
        }
    }

    @Override
    public void update(Number obj) {
        if (sample()) {
            this.unFlushed.update(obj.longValue());
        }
    }

    @Override
    public void updateDirectly(Number obj) {
        this.unFlushed.update(obj.longValue());
    }

    @Override
    public Map<Integer, Histogram> getWindowMetricMap() {
        return histogramMap;
    }

    @Override
    public Histogram mkInstance() {
        return newHistogram();
    }

    @Override
    protected void updateSnapshot(int window) {
        Histogram histogram = histogramMap.get(window);
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
        for (Histogram histogram : histogramMap.values()) {
            for (long val : values) {
                histogram.update(val);
            }
        }
        for (long val : values) {
            for (AsmMetric metric : this.assocMetrics) {
                metric.updateDirectly(val);
            }
        }
        this.unFlushed = newHistogram();
    }

    @Override
    public AsmMetric clone() {
        return new AsmHistogram();
    }

    private Histogram newHistogram() {
        return new Histogram(new ExponentiallyDecayingReservoir());
    }
}
