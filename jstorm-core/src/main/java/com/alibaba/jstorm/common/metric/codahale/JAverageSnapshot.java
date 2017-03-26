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
    public double getValue(double quantile) {
        //ingore
        return 0.0;
    }

    @Override
    public long[] getValues() {
        //ingore
        return new long[0];
    }

    @Override
    public int size() {
        //ingore
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
        //ingore
    }

    public MetricSnapshot getMetricSnapshot() {
        return metricSnapshot;
    }

    public void setMetricSnapshot(MetricSnapshot metricSnapshot) {
        this.metricSnapshot = metricSnapshot;
    }
}
