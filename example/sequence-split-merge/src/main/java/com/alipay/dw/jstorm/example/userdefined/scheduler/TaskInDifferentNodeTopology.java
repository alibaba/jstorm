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
package com.alipay.dw.jstorm.example.userdefined.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * This is a basic example to check whether all workers run on the user-defined
 * host
 */
public class TaskInDifferentNodeTopology {
    static List<AtomicLong>     counters  = new ArrayList<>();
    private static final String BOLT_NAME = "bolt";
    
    public static class ExclamationLoggingBolt extends BaseRichBolt {
        OutputCollector _collector;
        Logger          _rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        // ensure the loggers are configured in the worker.xml before
        // trying to use them here
        Logger          _logger     = LoggerFactory.getLogger("com.myapp");
        Logger          _subLogger  = LoggerFactory.getLogger("com.myapp.sub");
        
        AtomicLong counter;
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            counter = new AtomicLong(0);
            counters.add(counter);
        }
        
        @Override
        public void execute(Tuple tuple) {
            
            _collector.ack(tuple);
            
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
    
    static boolean      isLocal     = true;
    static List<String> hosts;
    static boolean      spoutSingle = true;
    static Config       conf        = JStormHelper.getConfig(null);
    
    public static void test() throws Exception {
        JStormHelper.cleanCluster();
        hosts = JStormHelper.getSupervisorHosts();
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("word", new TestWordSpout(), 1);
        /*********
         * 
         * This make sure the tasks will run on different nodes
         * 
         * 
         * 
         */
        Map<String, Object> componentMap = new HashMap<>();
        ConfigExtension.setTaskOnDifferentNode(componentMap, true);
        builder.setBolt(BOLT_NAME, new ExclamationLoggingBolt(), hosts.size()).localFirstGrouping("word")
                .addConfigurations(componentMap);
                
        if (isLocal == false) {
            
            if (spoutSingle == true) {
                conf.setNumWorkers(hosts.size() + 3);
            } else {
                conf.setNumWorkers(hosts.size());
            }
            
        }
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 180,
                    new Validator(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        isLocal = false;
        conf = JStormHelper.getConfig(args);
        if (args.length != 0) {
            spoutSingle = false;
        }
        test();
    }
    
    public static class Validator implements Callback {
        
        private Config conf;
        private String topologyName;
        
        public Validator(Config conf) {
            this.conf = conf;
        }
        
        public void verifyAssignment() {
            Map<String, Double> workerMetrics = JStormUtils.getMetrics(conf, topologyName, MetaType.WORKER, null);
            
            Set<String> boltHosts = new HashSet<String>();
            for (String key : workerMetrics.keySet()) {
                String[] tags = key.split("@");
                boltHosts.add(tags[2]);
            }
            
            if (boltHosts.size() != hosts.size()) {
                Assert.fail("Failed to do task in different node");
            }
        }
        
        public void verifyNumber() {
            Map<String, Double> taskMetrics = JStormUtils.getMetrics(conf, topologyName, null, null);
            
            boolean receivedTuple = false;
            for (Entry<String, Double> entry : taskMetrics.entrySet()) {
                String key = entry.getKey();
                Double value = entry.getValue();
                
                if (key.indexOf(MetricDef.ACKED_NUM) <= 0) {
                    continue;
                }
                
                if (key.indexOf(BOLT_NAME) <= 0) {
                    continue;
                }
                
                if (spoutSingle) {
                    
                    Assert.assertTrue(key + " should receive tuples ", value > 0.0);
                    receivedTuple = true;
                } else {
                    if (value > 0.0) {
                        if (receivedTuple == false) {
                            receivedTuple = true;
                        } else {
                            Assert.fail("Should only one task can receive tuple");
                        }
                    }
                }
            }
            
            Assert.assertTrue(BOLT_NAME + " should receive tuple ", receivedTuple);
        }
        
        @Override
        public <T> Object execute(T... args) {
            // TODO Auto-generated method stub
            
            topologyName = (String) args[0];
            
            if (isLocal == true) {
                Callback callback = new JStormHelper.CheckAckedFail(conf);
                callback.execute(args);
                return null;
            }
            
            
            verifyAssignment();
            
            verifyNumber();
            
            return null;
            
        }
        
    }
}
