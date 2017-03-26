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
package org.apache.storm.starter;

import com.alibaba.starter.utils.Assert;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology is a basic example of doing distributed RPC on top of Storm. It
 * implements a function that appends a "!" to any string you send the DRPC
 * function.
 *
 * @see <a href="http://storm.apache.org/documentation/Distributed-RPC.html">
 *      Distributed RPC</a>
 */
public class BasicDRPCTopology {
    public static class ExclaimBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String input = tuple.getString(1);
            collector.emit(new Values(tuple.getValue(0), input + "!"));
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
        
    }
    
    public static void testDrpc() {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);
        
        Config conf = new Config();
        
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));
        
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
        }
        try {
            for (String word : new String[] { "hello", "goodbye" }) {
                System.out.println("Result for \"" + word + "\": " + drpc.execute("exclamation", word));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to run DRPC Test");
        }
        
        drpc.shutdown();
        cluster.shutdown();
    }
    
    public static void main(String[] args) throws Exception {
        
        testDrpc();
    }
}
