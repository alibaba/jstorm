package com.jstorm.example.unittests.order;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.MetricClient;

import java.util.HashMap;
import java.util.Map;

/**
 * @author binyang.dby
 */
public class InOrderTestBolt extends BaseBasicBolt
{
    private Map<Integer, Integer> expected = new HashMap<>();   //store the taskIndex and the content has received before.
    private transient MetricClient metricClient;
    private transient AsmCounter successCounter;
    private transient AsmCounter failCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        super.prepare(stormConf, context);
        metricClient = new MetricClient(context);
        successCounter = metricClient.registerCounter(InOrderTestMetricsDef.METRIC_BOLT_SUCCESS);
        successCounter.setOp(AsmMetric.MetricOp.LOG & AsmMetric.MetricOp.REPORT);
        failCounter = metricClient.registerCounter(InOrderTestMetricsDef.METRIC_BOLT_FAIL);
        failCounter.setOp(AsmMetric.MetricOp.LOG & AsmMetric.MetricOp.REPORT);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer c1 = input.getInteger(0);
        Integer c2 = input.getInteger(1);

        Integer expect = expected.get(c1);
        if (expect == null)
            expect = 0;

        if (c2.intValue() == expect.intValue())
            successCounter.inc();
        else
            failCounter.inc();

        expect = c2 + 1;
        expected.put(c1, expect);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
