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
package com.jstorm.example.unittests.sequence;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.PairMaker;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author binyang.dby on 2016/7/8.
 */
public class SequenceTestSpout implements IRichSpout
{
    private MetricClient metricClient;
    private AsmCounter emitCounter;
    private AsmCounter successCounter;
    private AsmCounter failCounter;
    private AsmCounter tradeSumCounter;
    private AsmCounter customerSumCounter;

    private SpoutOutputCollector collector;
    private boolean isFinished = false;
    private static long SPOUT_MAX_SEND_NUM;
    private final static long MAX_PENDING_COUNTER = Long.MAX_VALUE;
    private int tupleId;
    private Random idGenerator;
    private AtomicLong handleCounter = new AtomicLong(0);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "RECORD"));
        declarer.declareStream(SequenceTopologyDef.CONTROL_STREAM_ID, new Fields("CONTROL"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.metricClient = new MetricClient(context);
        this.emitCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_SPOUT_EMIT);
        this.successCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_SPOUT_SUCCESS);
        this.failCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_SPOUT_FAIL);
        this.tradeSumCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_SPOUT_TRADE_SUM);
        this.customerSumCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_SPOUT_CUSTOMER_SUM);
        this.isFinished = false;
        this.idGenerator = new Random();
        SPOUT_MAX_SEND_NUM = (long)conf.get("spout.max.sending.num");

        JStormUtils.sleepMs(10 * 1000);
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if (isFinished) {
            long now = System.currentTimeMillis();
            String ctrlMsg = "spout don't send message due to pending num at " + now;
            collector.emit(SequenceTopologyDef.CONTROL_STREAM_ID, new Values(String.valueOf(now)), ctrlMsg);

            JStormUtils.sleepMs(1000);
            return;
        }

        if (tupleId >= SPOUT_MAX_SEND_NUM) {
            isFinished = true;
            return;
        }

        emit();
    }

    public void emit() {
        emitCounter.inc();
        Pair trade = PairMaker.makeTradeInstance();
        Pair customer = PairMaker.makeCustomerInstance();

        TradeCustomer tradeCustomer = new TradeCustomer();
        tradeCustomer.setTrade(trade);
        tradeCustomer.setCustomer(customer);
        tradeCustomer.setBuffer(null);

        tradeSumCounter.update(trade.getValue());
        customerSumCounter.update(customer.getValue());

        collector.emit(new Values(idGenerator.nextLong(), tradeCustomer), tupleId);
        tupleId++;
        handleCounter.incrementAndGet();
        while (handleCounter.get() >= MAX_PENDING_COUNTER - 1) {
            JStormUtils.sleepMs(1);
        }
    }

    @Override
    public void ack(Object msgId) {
        handleCounter.decrementAndGet();
        successCounter.inc();
    }

    @Override
    public void fail(Object msgId) {
        handleCounter.decrementAndGet();
        failCounter.inc();
    }
}
