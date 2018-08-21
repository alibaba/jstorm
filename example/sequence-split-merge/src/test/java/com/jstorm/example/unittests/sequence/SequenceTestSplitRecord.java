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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/8.
 */
public class SequenceTestSplitRecord implements IBasicBolt
{
    private MetricClient metricClient;
    private AsmCounter emitCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        metricClient = new MetricClient(context);
        emitCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_SPLIT_EMIT);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long tupleId = input.getLong(0);
        Object object = input.getValue(1);

        if (object instanceof TradeCustomer) {

            TradeCustomer tradeCustomer = (TradeCustomer) object;

            Pair trade = tradeCustomer.getTrade();
            Pair customer = tradeCustomer.getCustomer();

            collector.emit(SequenceTopologyDef.TRADE_STREAM_ID, new Values(tupleId, trade));
            collector.emit(SequenceTopologyDef.CUSTOMER_STREAM_ID, new Values(tupleId, customer));
            emitCounter.update(2);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SequenceTopologyDef.TRADE_STREAM_ID, new Fields("ID", "TRADE"));
        declarer.declareStream(SequenceTopologyDef.CUSTOMER_STREAM_ID, new Fields("ID", "CUSTOMER"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
