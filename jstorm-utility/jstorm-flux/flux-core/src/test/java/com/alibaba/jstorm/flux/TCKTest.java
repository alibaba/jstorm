/*
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
package com.alibaba.jstorm.flux;


import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import com.alibaba.jstorm.flux.model.ExecutionContext;
import com.alibaba.jstorm.flux.model.TopologyDef;
import com.alibaba.jstorm.flux.parser.FluxParser;
import com.alibaba.jstorm.flux.test.TestBolt;
import org.junit.Test;


import static org.junit.Assert.*;

public class TCKTest {

    @Test
    public void testTopologySourceWithConfigMethods() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/config-methods-test.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();

        // make sure the property was actually set
        TestBolt bolt = (TestBolt)context.getBolt("bolt-1");
        assertTrue(bolt.getFoo().equals("foo"));
        assertTrue(bolt.getBar().equals("bar"));
        assertTrue(bolt.getFooBar().equals("foobar"));
    }
}
