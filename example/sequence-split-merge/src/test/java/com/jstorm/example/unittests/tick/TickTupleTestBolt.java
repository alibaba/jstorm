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
package com.jstorm.example.unittests.tick;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmMeter;
import com.alibaba.jstorm.metric.MetricClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/21.
 */
public class TickTupleTestBolt implements IRichBolt{
    private Logger LOG = LoggerFactory.getLogger(TickTupleTestBolt.class);

    private MetricClient metricClient;
    private AsmCounter nonTickCounter;
    private AsmMeter tickMeter;

    private long lastMills = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.metricClient = new MetricClient(topologyContext);
        this.nonTickCounter = metricClient.registerCounter("TickTupleTest.NonTickCounter");
        this.tickMeter = metricClient.registerMeter("TickTupleTest.TickMeter");
    }

    @Override
    public void execute(Tuple tuple) {
        if(TupleHelpers.isTickTuple(tuple)) {
            LOG.info("Tick! Seconds from last receive = " + (System.currentTimeMillis() - lastMills)/1000f);
            lastMills = System.currentTimeMillis();
            tickMeter.mark();
        }
        else
            nonTickCounter.inc();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
