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

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.utils.JStormUtils;
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
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TridentFastWordCount {
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";
    
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
                new Values("to be or not to be the person"),
                new Values("marry had a little lamb whos fleese was white as snow"),
                new Values("and every where that marry went the lamb was sure to go"),
                new Values("one two three four five six seven eight nine ten"),
                new Values("this is a test of the emergency broadcast system this is only a test"),
                new Values("peter piper picked a peck of pickeled peppers"),
                new Values("JStorm is a distributed and fault-tolerant realtime computation system."),
                new Values(
                        "Inspired by Apache Storm, JStorm has been completely rewritten in Java and provides many more enhanced features."),
                new Values("JStorm has been widely used in many enterprise environments and proved robust and stable."),
                new Values("JStorm provides a distributed programming framework very similar to Hadoop MapReduce."),
                new Values(
                        "The developer only needs to compose his/her own pipe-lined computation logic by implementing the JStorm API"),
                new Values(" which is fully compatible with Apache Storm API"),
                new Values("and submit the composed Topology to a working JStorm instance."),
                new Values("Similar to Hadoop MapReduce, JStorm computes on a DAG (directed acyclic graph)."),
                new Values("Different from Hadoop MapReduce, a JStorm topology runs 24 * 7"),
                new Values("the very nature of its continuity abd 100% in-memory architecture "),
                new Values(
                        "has been proved a particularly suitable solution for streaming data and real-time computation."),
                new Values("JStorm guarantees fault-tolerance."), new Values("Whenever a worker process crashes, "),
                new Values(
                        "the scheduler embedded in the JStorm instance immediately spawns a new worker process to take the place of the failed one."),
                new Values(" The Acking framework provided by JStorm guarantees that every single piece of data will be processed at least once.") );
        spout.setCycle(true);
        
        
        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 2);
        int count_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 2);
        
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(spout_Parallelism_hint)
                .each(new Fields("sentence"), new Split(), new Fields("word")).parallelismHint(split_Parallelism_hint).groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(count_Parallelism_hint);
                
        return topology.build();
    }
    
    static boolean   isLocal = true;
    static LocalDRPC drpc    = null;
    static Config    conf    = JStormHelper.getConfig(null);
    static {
        conf.put(Config.STORM_CLUSTER_MODE, "local");
    }
    
    
    public static void test() {
        TopologyBuilder builder = new TopologyBuilder();
        
        
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
