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
package com.jstorm.example.unittests.drpc;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import com.jstorm.example.unittests.utils.JStormUnitTestDRPCValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.apache.storm.starter.BasicDRPCTopology;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/22.
 */
public class BasicDRPCTopologyTest {
    @Test
    public void testBasicDRPCTopology()
    {
        LinearDRPCTopologyBuilder topologyBuilder = new LinearDRPCTopologyBuilder("exclamation");
        topologyBuilder.addBolt(new BasicDRPCTopology.ExclaimBolt(), 3);

        Config config = new Config();
        config.put(Config.TOPOLOGY_NAME, "BasicDRPCTopologyTest");

        LocalDRPC localDRPC = new LocalDRPC();
        JStormUnitTestDRPCValidator validator = new JStormUnitTestDRPCValidator(localDRPC) {
            @Override
            public boolean validate(Map config)
            {
                String result = executeLocalDRPC("exclamation", "hello");
                assertEquals("hello!", result);

                result = executeLocalDRPC("exclamation", "goodbye");
                assertEquals("goodbye!", result);

                return true;
            }
        };

        try {
            JStormUnitTestRunner.submitTopology(topologyBuilder.createLocalTopology(localDRPC), config, 120, validator);
        }
        finally {
            localDRPC.shutdown();
        }
    }
}
