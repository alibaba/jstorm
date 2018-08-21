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
package com.jstorm.example.unittests.batch;

import backtype.storm.Config;
import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;

/**
 * @author binyang.dby on 2016/7/20.
 */
public class SimpleBatchTopologyTest
{
    @Test
    public void testSimpleBatchTopology() {
        BatchTopologyBuilder batchTopologyBuilder = new BatchTopologyBuilder("SimpleBatchTopology");
        batchTopologyBuilder.setSpout("batchSpout", new SimpleBatchTestSpout(), 1);
        batchTopologyBuilder.setBolt("batchBolt", new SimpleBatchTestBolt(), 2).shuffleGrouping("batchSpout");

        Config config = new Config();
        config.put(Config.TOPOLOGY_NAME, "SimpleBatchTopologyTest");
        config.setMaxSpoutPending(1);

        JStormUnitTestRunner.submitTopology(batchTopologyBuilder.getTopologyBuilder().createTopology(), config, 120, null);
    }
}
