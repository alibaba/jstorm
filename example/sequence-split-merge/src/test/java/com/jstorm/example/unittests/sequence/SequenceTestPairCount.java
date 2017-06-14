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

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/8.
 */
public class SequenceTestPairCount implements IBasicBolt
{
    private MetricClient metricClient;
    private AsmCounter emitCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        metricClient = new MetricClient(context);
        if(context.getThisComponentId().equals(SequenceTopologyDef.TRADE_BOLT_NAME))
            emitCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_PAIR_TRADE_EMIT);
        else if(context.getThisComponentId().equals(SequenceTopologyDef.CUSTOMER_BOLT_NAME))
            emitCounter = metricClient.registerCounter(SequenceTestMetricsDef.METRIC_PAIR_CUSTOMER_EMIT);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long tupleId = input.getLong(0);
        Pair pair = (Pair) input.getValue(1);
        emitCounter.inc();
        collector.emit(new Values(tupleId, pair));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "PAIR"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
