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
import backtype.storm.topology.base.BaseWindowedBolt;
import com.jstorm.example.unittests.utils.JStormUnitTestMetricValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import com.jstorm.example.unittests.utils.JStormUnitTestValidator;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author binyang.dby on 2016/7/21.
 *
 * @Test pass at 2016/07/22
 */
public class SlidingTupleTsTopologyTest {
    public final static int WINDOW_LENGTH_SEC = 5;
    public final static int WINDOW_SLIDE_SEC = 3;
    public final static int WINDOW_LAG_SEC = 5;
    public final static int SPOUT_LIMIT = 300;

    @Test
    public void testSlidingTupleTsTopology()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        BaseWindowedBolt bolt = new SlidingTupleTestBolt()
                .withWindow(new BaseWindowedBolt.Duration(WINDOW_LENGTH_SEC, TimeUnit.SECONDS),
                        new BaseWindowedBolt.Duration(WINDOW_SLIDE_SEC, TimeUnit.SECONDS))
                .withTimestampField("ts").withLag(new BaseWindowedBolt.Duration(WINDOW_LAG_SEC, TimeUnit.SECONDS));
        topologyBuilder.setSpout("spout", new SlidingTupleTestRandomSpout(SPOUT_LIMIT), 1);
        topologyBuilder.setBolt("sum", bolt, 1).shuffleGrouping("spout");

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "SlidingTupleTsTopologyTest");

        Set<String> userDefineMetrics = new HashSet<String>();
        userDefineMetrics.add("SlidingTupleTsTopologyTest.SpoutSum");
        userDefineMetrics.add("SlidingTupleTsTopologyTest.BoltSum");

        JStormUnitTestValidator validator = new JStormUnitTestMetricValidator(userDefineMetrics) {
            @Override
            public boolean validateMetrics(Map<String, Double> metrics) {
                int spoutSum = (int) metrics.get("SlidingTupleTsTopologyTest.SpoutSum").doubleValue();
                int boltSum = (int) metrics.get("SlidingTupleTsTopologyTest.BoltSum").doubleValue();
                assertEquals(spoutSum, boltSum);

                return true;
            }
        };

        JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 120, validator);
    }
}
