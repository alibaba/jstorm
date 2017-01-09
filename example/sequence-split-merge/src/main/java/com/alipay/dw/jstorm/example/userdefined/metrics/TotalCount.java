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
package com.alipay.dw.jstorm.example.userdefined.metrics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMeter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.starter.utils.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;
import com.alibaba.jstorm.metrics.Gauge;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This document demonstrate how to do user-define metrics and how to report one warning
 *
 * @author longda
 */
public class TotalCount implements IRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TotalCount.class);

    private OutputCollector collector;
    private TopologyContext context;
    private TpsCounter tpsCounter;
    private long lastTupleId = -1;

    private boolean checkTupleId = false;
    private boolean slowDonw = false;

    /**
     * User defined metrics
     */
    private MetricClient metricClient;
    private AsmCounter myCounter;
    private AsmMeter myMeter;
    private AsmHistogram myJStormHistogram;
    private AsmGauge myGauge;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());

        checkTupleId = JStormUtils.parseBoolean(stormConf.get("bolt.check.tupleId"), false);
        slowDonw = JStormUtils.parseBoolean(stormConf.get("bolt.slow.down"), false);

        metricClient = new MetricClient(context);

        Gauge<Double> gauge = new Gauge<Double>() {
            private Random random = new Random();

            @Override
            public Double getValue() {
                return random.nextDouble();
            }

        };
        myGauge = metricClient.registerGauge("myGauge", gauge);
        myCounter = metricClient.registerCounter("myCounter");
        myMeter = metricClient.registerMeter("myMeter");
        myJStormHistogram = metricClient.registerHistogram("myHistogram");

        LOG.info("Finished preparation " + stormConf);
    }

    private AtomicLong tradeSum = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(1);

    @Override
    public void execute(Tuple input) {
        if (TupleHelpers.isTickTuple(input)) {
            LOG.info("Receive one Ticket Tuple " + input.getSourceComponent());
            return;
        }
        if (input.getSourceStreamId().equals(SequenceTopologyDef.CONTROL_STREAM_ID)) {
            String str = (input.getStringByField("CONTROL"));
            LOG.warn(str);
            return;
        }

        long before = System.currentTimeMillis();
        myCounter.update(1);
        myMeter.update(1);

        if (checkTupleId) {
            Long tupleId = input.getLong(0);
            if (tupleId <= lastTupleId) {

                /***
                 * Display warning
                 */
                String errorMessage = ("LastTupleId is " + lastTupleId + ", but now:" + tupleId);

                JStormUtils.reportError(context, errorMessage);
            }
            lastTupleId = tupleId;
        }

        TradeCustomer tradeCustomer;
        try {
            tradeCustomer = (TradeCustomer) input.getValue(1);
        } catch (Exception e) {
            LOG.error(input.getSourceComponent() + "  " + input.getSourceTask() + " " + input.getSourceStreamId()
                    + " target " + (input));
            throw new RuntimeException(e);
        }

        tradeSum.addAndGet(tradeCustomer.getTrade().getValue());
        customerSum.addAndGet(tradeCustomer.getCustomer().getValue());

        collector.ack(input);

        long now = System.currentTimeMillis();
        long spend = now - tradeCustomer.getTimestamp();

        tpsCounter.count(spend);
        myJStormHistogram.update(now - before);

        if (slowDonw) {
            JStormUtils.sleepMs(20);
        }

    }

    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
        LOG.info("Finish cleanup");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
