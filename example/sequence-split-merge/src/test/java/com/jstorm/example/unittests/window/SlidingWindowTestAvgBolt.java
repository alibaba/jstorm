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
package com.jstorm.example.unittests.window;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.windowing.TupleWindow;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;

import java.util.List;
import java.util.Map;

/**
 * @author binyang.dby on 2016/7/21.
 */
public class SlidingWindowTestAvgBolt extends BaseWindowedBolt {
    private MetricClient metricClient;
    private AsmCounter asmCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.metricClient = new MetricClient(context);
        this.asmCounter = metricClient.registerCounter("SlidingWindowTopologyTest.BoltAvgSum");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        int sum = 0;
        List<Tuple> tuplesInWindow = inputWindow.get();
        if (tuplesInWindow.size() > 0) {
            for (Tuple tuple : tuplesInWindow) {
                sum += (int) tuple.getValue(0);
            }
            asmCounter.update(sum / tuplesInWindow.size());
        }
    }
}
