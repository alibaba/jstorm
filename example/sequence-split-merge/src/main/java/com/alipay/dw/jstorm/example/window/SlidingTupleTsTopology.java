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
package com.alipay.dw.jstorm.example.window;

import java.util.concurrent.TimeUnit;

import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.starter.bolt.SlidingWindowSumBolt;
import org.apache.storm.starter.spout.RandomIntegerSpout;

import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * Windowing based on tuple timestamp (e.g. the time when tuple is generated
 * rather than when its processed).
 */
public class SlidingTupleTsTopology {
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        try {
            TopologyBuilder builder = new TopologyBuilder();
            BaseWindowedBolt bolt = new SlidingWindowSumBolt()
                    .withWindow(new Duration(5, TimeUnit.SECONDS), new Duration(3, TimeUnit.SECONDS))
                    .withTimestampField("ts").withLag(new Duration(5, TimeUnit.SECONDS));
            builder.setSpout("integer", new RandomIntegerSpout(), 1);
            builder.setBolt("slidingsum", bolt, 1).shuffleGrouping("integer");
            builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("slidingsum");
            
            conf.setDebug(true);
            
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        isLocal = false;
        conf = JStormHelper.getConfig(args);
        test();
    }
}
