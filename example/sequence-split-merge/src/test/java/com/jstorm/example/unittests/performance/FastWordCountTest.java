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
package com.jstorm.example.unittests.performance;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author binyang.dby on 2016/7/11.
 *
 * @Test pass at 2016/7/19
 */
public class FastWordCountTest {
    @Test
    public void testFastWordCount()
    {
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 1;
        int count_Parallelism_hint = 2;

        TopologyBuilder builder = new TopologyBuilder();

        boolean isLocalShuffle = false;

        builder.setSpout("spout", new FastWordCountTopology.FastRandomSentenceSpout(), spout_Parallelism_hint);
        if (isLocalShuffle)
            builder.setBolt("split", new FastWordCountTopology.SplitSentence(), split_Parallelism_hint).localFirstGrouping("spout");
        else
            builder.setBolt("split", new FastWordCountTopology.SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new FastWordCountTopology.WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "FastWordCountTest");

        JStormUnitTestRunner.submitTopology(builder.createTopology(), config, 60, null);
    }
}
