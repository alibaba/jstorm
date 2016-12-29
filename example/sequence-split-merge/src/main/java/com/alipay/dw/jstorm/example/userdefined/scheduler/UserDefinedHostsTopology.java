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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This is a basic example to check whether all workers run on the user-defined
 * host
 */
public class UserDefinedHostsTopology {
    static List<AtomicLong> counters = new ArrayList<>();
    
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
            _rootLogger.debug("root: This is a DEBUG message");
            _rootLogger.info("root: This is an INFO message");
            _rootLogger.warn("root: This is a WARN message");
            _rootLogger.error("root: This is an ERROR message");
            
            _logger.debug("myapp: This is a DEBUG message");
            _logger.info("myapp: This is an INFO message");
            _logger.warn("myapp: This is a WARN message");
            _logger.error("myapp: This is an ERROR message");
            
            _subLogger.debug("myapp.sub: This is a DEBUG message");
            _subLogger.info("myapp.sub: This is an INFO message");
            _subLogger.warn("myapp.sub: This is a WARN message");
            _subLogger.error("myapp.sub: This is an ERROR message");
            
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
            counter.incrementAndGet();
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
    
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationLoggingBolt(), 3).noneGrouping("word");
        builder.setBolt("exclaim2", new ExclamationLoggingBolt(), 2).shuffleGrouping("exclaim1");
        
        String hostname = NetWorkUtils.hostname();
        List<String> hosts = new ArrayList<String>();
        hosts.add(hostname);
        
        /*********
         * 
         * This make sure all worker run on the user-defined hosts
         * 
         * 
         * 
         */
        conf.put(Config.ISOLATION_SCHEDULER_MACHINES, hosts);
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                    new Validator(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static class Validator implements Callback {
        
        private Config conf;
        
        public Validator(Config conf) {
            this.conf = conf;
        }
        
        @Override
        public <T> Object execute(T... args) {
            // TODO Auto-generated method stub
            String topologyName = (String) args[0];
            String ip = NetWorkUtils.ip();
            
            Callback callback = new JStormHelper.CheckAckedFail(conf);
            callback.execute(args);
            
            if (isLocal == false) {
                JStormUtils.sleepMs(60 * 1000);
                Set<String> hosts = new HashSet<String>();
                Set<String> workerSlots = new HashSet<String>();
                Map<String, Double> metrics = JStormUtils.getMetrics(conf, topologyName, MetaType.WORKER, null);
                
                for (String metricName : metrics.keySet()) {
                    String[] tag = metricName.split("@");
                    String host = tag[2];
                    String port = tag[3];
                    
                    hosts.add(host);
                    workerSlots.add(host + ":" + port);
                }
                
                int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 3);
                
                Assert.assertTrue(topologyName + " worker number isn't correct", workerNum == workerSlots.size());
                
                for (String host : hosts) {
                    Assert.assertTrue(topologyName + " worker isn't run on " + ip, host.equals(ip));
                }
            }
            
            return null;
            
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        isLocal = false;
        conf = JStormHelper.getConfig(args);
        test();
    }
}
