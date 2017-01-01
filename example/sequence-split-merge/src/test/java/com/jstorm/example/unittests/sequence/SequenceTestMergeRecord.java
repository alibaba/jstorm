package com.jstorm.example.unittests.sequence;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by binyang.dby on 2016/7/8.
 */
public class SequenceTestMergeRecord implements IRichBolt
{
    private Map<Long, Tuple> tradeMap    = new HashMap<Long, Tuple>();
    private Map<Long, Tuple> customerMap = new HashMap<Long, Tuple>();
    private AtomicLong tradeSum    = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(0);
    private OutputCollector collector;
    private MetricClient metricClient;
    private AsmCounter emitCounter;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        metricClient = new MetricClient(context);
        emitCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_MERGE_EMIT);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Long tupleId = input.getLong(0);
        Pair pair = (Pair) input.getValue(1);

        Pair trade = null;
        Pair customer = null;

        Tuple tradeTuple = null;
        Tuple customerTuple = null;

        if (input.getSourceComponent().equals(SequenceTopologyDef.CUSTOMER_BOLT_NAME)) {
            customer = pair;
            customerTuple = input;

            tradeTuple = tradeMap.remove(tupleId);
            if (tradeTuple == null) {
                customerMap.put(tupleId, input);
                return;
            }

            trade = (Pair) tradeTuple.getValue(1);

        } else if (input.getSourceComponent().equals(SequenceTopologyDef.TRADE_BOLT_NAME)) {
            trade = pair;
            tradeTuple = input;

            customerTuple = customerMap.remove(tupleId);
            if (customerTuple == null) {
                tradeMap.put(tupleId, input);
                return;
            }

            customer = (Pair) customerTuple.getValue(1);
        } else {
            collector.fail(input);
            return;
        }

        tradeSum.addAndGet(trade.getValue());
        customerSum.addAndGet(customer.getValue());

        collector.ack(tradeTuple);
        collector.ack(customerTuple);

        TradeCustomer tradeCustomer = new TradeCustomer();
        tradeCustomer.setTrade(trade);
        tradeCustomer.setCustomer(customer);
        collector.emit(new Values(tupleId, tradeCustomer));
        emitCounter.inc();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "RECORD"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
