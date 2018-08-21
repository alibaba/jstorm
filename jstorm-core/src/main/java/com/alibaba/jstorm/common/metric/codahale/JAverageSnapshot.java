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
package com.alibaba.jstorm.common.metric.codahale;

import backtype.storm.generated.MetricSnapshot;
import com.codahale.metrics.Snapshot;

import java.io.OutputStream;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class JAverageSnapshot extends Snapshot {

    private MetricSnapshot metricSnapshot;

    public JAverageSnapshot() {
        metricSnapshot = new MetricSnapshot();
    }

    @Override
    public double getValue(double percentile) {
        return 0.0;
    }

    @Override
    public long[] getValues() {
        return new long[0];
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public double getMedian() {
        return metricSnapshot.get_p50();
    }

    @Override
    public double get75thPercentile() {
        return metricSnapshot.get_p75();
    }

    @Override
    public double get95thPercentile() {
        return metricSnapshot.get_p95();
    }

    @Override
    public double get98thPercentile() {
        return metricSnapshot.get_p98();
    }

    @Override
    public double get99thPercentile() {
        return metricSnapshot.get_p99();
    }

    @Override
    public double get999thPercentile() {
        return metricSnapshot.get_p999();
    }

    @Override
    public long getMax() {
        return metricSnapshot.get_max();
    }

    @Override
    public double getMean() {
        return metricSnapshot.get_mean();
    }

    @Override
    public long getMin() {
        return metricSnapshot.get_min();
    }

    @Override
    public double getStdDev() {
        return metricSnapshot.get_stddev();
    }

    @Override
    public void dump(OutputStream output) {
    }

    public MetricSnapshot getMetricSnapshot() {
        return metricSnapshot;
    }

    public void setMetricSnapshot(MetricSnapshot metricSnapshot) {
        this.metricSnapshot = metricSnapshot;
    }
}
