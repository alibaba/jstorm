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
package org.apache.storm.starter.trident;

import org.apache.storm.starter.InOrderDeliveryTest.Check;
import org.apache.storm.starter.InOrderDeliveryTest.InOrderSpout;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TridentWordCount {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);
        
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);
                
        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }
    
    static boolean   isLocal = true;
    static LocalDRPC drpc    = null;
    static Config    conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        
        conf.setMaxSpoutPending(20);
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        if (isLocal) {
            drpc = new LocalDRPC();
        }
        
        try {
            JStormHelper.runTopology(buildTopology(drpc), topologyName, conf, 60, new JStormHelper.CheckAckedFail(conf),
                    isLocal);
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
    
    public static class DrpcValidator implements Callback {
        
        @Override
        public <T> Object execute(T... args) {
            // TODO Auto-generated method stub
            String topologyName = (String) args[0];
            try {
                if (isLocal) {
                    for (int i = 0; i < 100; i++) {
                        System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                        Thread.sleep(1000);
                    }
                } else {
                    Callback callback = new JStormHelper.CheckAckedFail(conf);
                    callback.execute(args);
                    
                    // Map conf = Utils.readStormConfig();
                    // "foo.com/blog/1" "engineering.twitter.com/blog/5"
                    // DRPCClient client = new DRPCClient(conf, "localhost",
                    // 4772);
                    // String result = client.execute((String)args[0],
                    // "tech.backtype.com/blog/123");
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Failed to run drpc action");
                
            }
            return null;
        }
        
    }
}
