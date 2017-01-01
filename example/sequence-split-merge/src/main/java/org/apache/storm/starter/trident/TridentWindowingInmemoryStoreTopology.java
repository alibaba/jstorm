/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.starter.trident;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Consumer;
import storm.trident.testing.CountAsAggregator;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import storm.trident.windowing.InMemoryWindowsStoreFactory;
import storm.trident.windowing.WindowsStoreFactory;
import storm.trident.windowing.config.SlidingCountWindow;
import storm.trident.windowing.config.SlidingDurationWindow;
import storm.trident.windowing.config.TumblingCountWindow;
import storm.trident.windowing.config.TumblingDurationWindow;
import storm.trident.windowing.config.WindowConfig;

/**
 * Sample application of trident windowing which uses inmemory store for storing
 * tuples in window.
 */
public class TridentWindowingInmemoryStoreTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TridentWindowingInmemoryStoreTopology.class);
    
    public static StormTopology buildTopology(WindowsStoreFactory windowStore, WindowConfig windowConfig)
            throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);
        
        TridentTopology topology = new TridentTopology();
        
        Stream stream = topology.newStream("spout1", spout).parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .window(windowConfig, windowStore, new Fields("word"), new CountAsAggregator(), new Fields("count"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        LOG.info("Received tuple: [{}]", input);
                    }
                });
                
        return topology.build();
    }
    
    static boolean   isLocal = true;
    static LocalDRPC drpc    = null;
    static Config    conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        try {
            WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();
            
            List<? extends WindowConfig> list = Arrays.asList(SlidingCountWindow.of(1000, 100),
                    TumblingCountWindow.of(1000),
                    SlidingDurationWindow.of(new BaseWindowedBolt.Duration(6, TimeUnit.SECONDS),
                            new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS)),
                    TumblingDurationWindow.of(new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS)));
                    
            for (WindowConfig windowConfig : list) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordCounter", conf, buildTopology(mapState, windowConfig));
                Utils.sleep(60 * 1000);
                cluster.shutdown();
            }
            
            for (WindowConfig windowConfig : list) {
                String name = topologyName + "." + windowConfig.getWindowLength() + "."
                        + windowConfig.getSlidingLength();
                        
                JStormHelper.runTopology(buildTopology(mapState, windowConfig), name, conf, 60,
                        new JStormHelper.CheckAckedFail(conf), isLocal);
            }
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        isLocal = true;
        conf = JStormHelper.getConfig(args);
        test();
    }
    
}
