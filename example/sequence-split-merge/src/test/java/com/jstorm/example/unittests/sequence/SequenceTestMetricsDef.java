package com.jstorm.example.unittests.sequence;

/**
 * Created by binyang.dby on 2016/7/8.
 *
 * I use endsWith to filter the user define metrics from all metrics. Make sure all the metrics ends with ".user.define"
 */
public class SequenceTestMetricsDef
{
    public final static String METRIC_SPOUT_EMIT = "metric.seq.spout.emit.user.define";
    public final static String METRIC_SPOUT_SUCCESS = "metric.seq.spout.success.user.define";
    public final static String METRIC_SPOUT_FAIL = "metric.seq.spout.fail.user.define";
    public final static String METRIC_SPOUT_TRADE_SUM = "metric.seq.spout.trade.sum.user.define";
    public final static String METRIC_SPOUT_CUSTOMER_SUM = "metric.seq.spout.customer.sum.user.define";

    public final static String METRIC_SPLIT_EMIT = "metric.seq.split.emit.user.define";

    public final static String METRIC_PAIR_TRADE_EMIT = "metric.seq.pair.trade.emit.user.define";
    public final static String METRIC_PAIR_CUSTOMER_EMIT = "metric.seq.pair.customer.emit.user.define";

    public final static String METRIC_MERGE_EMIT = "metric.seq.merge.emit.user.define";

    public final static String METRIC_TOTAL_EXECUTE = "metric.seq.total.execute.user.define";
    public final static String METRIC_TOTAL_TRADE_SUM = "metric.seq.total.trade.sum.user.define";
    public final static String METRIC_TOTAL_CUSTOMER_SUM = "metric.seq.total.customer.sum.user.define";
}
