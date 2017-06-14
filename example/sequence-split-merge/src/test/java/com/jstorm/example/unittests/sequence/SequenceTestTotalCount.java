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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/8.
 */
public class SequenceTestTotalCount implements IRichBolt
{
    private MetricClient metricClient;
    private AsmCounter executeCounter;
    private AsmCounter tradeSumCounter;
    private AsmCounter customerSumCounter;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        metricClient = new MetricClient(context);
        executeCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_TOTAL_EXECUTE);
        tradeSumCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_TOTAL_TRADE_SUM);
        customerSumCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_TOTAL_CUSTOMER_SUM);
    }

    @Override
    public void execute(Tuple input) {
        executeCounter.inc();

        TradeCustomer tradeCustomer;
        try {
            tradeCustomer = (TradeCustomer) input.getValue(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        tradeSumCounter.update(tradeCustomer.getTrade().getValue());
        customerSumCounter.update(tradeCustomer.getCustomer().getValue());

        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
