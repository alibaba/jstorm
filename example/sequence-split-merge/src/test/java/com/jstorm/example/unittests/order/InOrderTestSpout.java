package com.jstorm.example.unittests.order;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author binyang.dby
 */
public class InOrderTestSpout extends BaseRichSpout
{
    private Logger LOG = LoggerFactory.getLogger(InOrderTestSpout.class);
    private long limit;
    private transient SpoutOutputCollector collector;
    private int task;
    private int content;
    private transient MetricClient metricClient;
    private transient AsmCounter emitCounter;

    public InOrderTestSpout(long limit) {
        this.limit = limit;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("c1", "c2"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.task = context.getThisTaskIndex();
        this.metricClient = new MetricClient(context);
        this.emitCounter = metricClient.registerCounter(InOrderTestMetricsDef.METRIC_SPOUT_EMIT);
        this.emitCounter.setOp(AsmMetric.MetricOp.LOG & AsmMetric.MetricOp.REPORT);
        LOG.info("open. task = " + task);
    }

    @Override
    public void nextTuple() {
        if(content >= limit) {
            JStormUtils.sleepMs(1000);
            return;
        }

        Values values = new Values(task, content);
        collector.emit(values, "msg");
        content++;
        emitCounter.inc();
        LOG.info("nextTuple. task = " + task + " content = " + content);
    }
}
