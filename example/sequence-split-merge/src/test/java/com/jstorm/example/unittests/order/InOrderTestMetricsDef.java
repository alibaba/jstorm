package com.jstorm.example.unittests.order;

/**
 * Created by binyang.dby on 2016/7/9.
 *
 * I use endsWith to filter the user define metrics from all metrics. Make sure all the metrics ends with ".user.define"
 */
public class InOrderTestMetricsDef
{
    public final static String METRIC_SPOUT_EMIT = "metric.order.spout.emit.user.define";
    public final static String METRIC_BOLT_SUCCESS = "metric.order.bolt.success.user.define";
    public final static String METRIC_BOLT_FAIL = "metric.order.bolt.fail.user.define";
}
