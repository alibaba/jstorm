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
package com.alipay.dw.jstorm.example.tm;

import java.util.Map;

import com.alibaba.jstorm.client.ConfigExtension;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class TMUdfStreamTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Map config = new Config();
        config.put(ConfigExtension.TOPOLOGY_MASTER_USER_DEFINED_STREAM_CLASS, "com.alipay.dw.jstorm.example.tm.TMUdfHandler");
        config.put(Config.TOPOLOGY_WORKERS, 2);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TMUdfSpout", new TMUdfSpout(), 2);
        builder.setBolt("TMUdfBolt", new TMUdfBolt(), 4);
        StormTopology topology = builder.createTopology();

        StormSubmitter.submitTopology("TMUdfTopology", config, topology);
    }
}