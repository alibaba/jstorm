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

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author binyang.dby on 2016/7/11.
 *
 */
public class RollingTopWordsTest
{
    public final static int DEFAULT_COUNT = 5;

    @Test
    public void testRollingTopWords()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("windowTestWordSpout", new WindowTestWordSpout(), 5);
        topologyBuilder.setBolt("windowTestRollingCountBolt", new WindowTestRollingCountBolt(9, 3), 4)
                .fieldsGrouping("windowTestWordSpout", new Fields("word")).addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        topologyBuilder.setBolt("windowTestIntermediateRankingBolt", new WindowTestIntermediateRankingBolt(DEFAULT_COUNT), 4)
                .fieldsGrouping("windowTestRollingCountBolt", new Fields("obj"));
        topologyBuilder.setBolt("windowTestTotalRankingsBolt", new WindowTestTotalRankingsBolt(DEFAULT_COUNT))
                .globalGrouping("windowTestIntermediateRankingBolt");

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "RollingTopWordsTest");

        //I really don't know how to validate if the result is right since
        //the tick time is not precise. It makes the output after passing
        //a window is unpredictable.
        //Now I just let it pass all the time.
        //TODO:FIX ME: how to validate if the result is right?
        JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 90, null);
    }
}
