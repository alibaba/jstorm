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
package com.alipay.dw.jstorm.example.performance.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * WordCount but teh spout does not stop, and the bolts are implemented in java.
 * This can show how fast the word count can run.
 */
public class FastWordCountTopology {
    private static Logger      LOG                             = LoggerFactory.getLogger(FastWordCountTopology.class);
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";
    
    public static class FastRandomSentenceSpout implements IRichSpout {
        SpoutOutputCollector _collector;
        Random               _rand;
        long                 sendingCount;
        long                 startTime;
        boolean              isStatEnable;
        int                  sendNumPerNexttuple;
        
        private static final String[] CHOICES = { "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers",
                "JStorm is a distributed and fault-tolerant realtime computation system.",
                "Inspired by Apache Storm, JStorm has been completely rewritten in Java and provides many more enhanced features.",
                "JStorm has been widely used in many enterprise environments and proved robust and stable.",
                "JStorm provides a distributed programming framework very similar to Hadoop MapReduce.", 
                "The developer only needs to compose his/her own pipe-lined computation logic by implementing the JStorm API",
                " which is fully compatible with Apache Storm API",
                "and submit the composed Topology to a working JStorm instance.",
                "Similar to Hadoop MapReduce, JStorm computes on a DAG (directed acyclic graph)." ,
                "Different from Hadoop MapReduce, a JStorm topology runs 24 * 7", 
                "the very nature of its continuity abd 100% in-memory architecture ",
                "has been proved a particularly suitable solution for streaming data and real-time computation.",
                "JStorm guarantees fault-tolerance.",
                "Whenever a worker process crashes, ",
                "the scheduler embedded in the JStorm instance immediately spawns a new worker process to take the place of the failed one.",
                " The Acking framework provided by JStorm guarantees that every single piece of data will be processed at least once." };
                
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
            sendingCount = 0;
            startTime = System.currentTimeMillis();
            sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
            isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        }
        
        @Override
        public void nextTuple() {
            int n = sendNumPerNexttuple;
            while (--n >= 0) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                _collector.emit(new Values(sentence));
            }
            updateSendTps();
        }
        
        @Override
        public void ack(Object id) {
            // Ignored
        }
        
        @Override
        public void fail(Object id) {
            _collector.emit(new Values(id), id);
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
        
        private void updateSendTps() {
            if (!isStatEnable)
                return;
                
            sendingCount++;
            long now = System.currentTimeMillis();
            long interval = now - startTime;
            if (interval > 60 * 1000) {
                LOG.info("Sending tps of last one minute is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
                startTime = now;
                sendingCount = 0;
            }
        }
        
        @Override
        public void close() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void activate() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void deactivate() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    public static class SplitSentence implements IRichBolt {
        OutputCollector collector;
        
        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word));
            }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
        
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        
        @Override
        public void cleanup() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    public static class WordCount implements IRichBolt {
        OutputCollector      collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();
        
        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
        }
        
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            
        }
        
        @Override
        public void cleanup() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    
    static {
        conf.put(Config.STORM_CLUSTER_MODE, "local");
    }
    
    public static void test() {
        
        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int count_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 2);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        boolean isLocalShuffle = JStormUtils.parseBoolean(conf.get("is.local.first.group"), false);
        
        builder.setSpout("spout", new FastRandomSentenceSpout(), spout_Parallelism_hint);
        if (isLocalShuffle)
            builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).localFirstGrouping("spout");
        else
            builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        isLocal = JStormHelper.localMode(conf);
        
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        conf = JStormHelper.getConfig(args);
        test();
    }
}
