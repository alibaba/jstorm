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
package com.alipay.dw.jstorm.example.batch;

import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class SimpleBatchTopology {
    
    public static TopologyBuilder SetBuilder() {
        BatchTopologyBuilder topologyBuilder = new BatchTopologyBuilder(topologyName);
        
        int spoutParallel = JStormUtils.parseInt(conf.get("topology.spout.parallel"), 1);
        
        BoltDeclarer boltDeclarer = topologyBuilder.setSpout("Spout", new SimpleSpout(), spoutParallel);
        
        int boltParallel = JStormUtils.parseInt(conf.get("topology.bolt.parallel"), 2);
        topologyBuilder.setBolt("Bolt", new SimpleBolt(), boltParallel).shuffleGrouping("Spout");
        
        return topologyBuilder.getTopologyBuilder();
    }
    
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    static String  topologyName;
    
    public static void test() {
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        topologyName = className[className.length - 1];
        
        try {
            JStormHelper.runTopology(SetBuilder().createTopology(), topologyName, conf, 100,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        conf = JStormHelper.getConfig(args);
        isLocal = false;
        test();
    }
}
