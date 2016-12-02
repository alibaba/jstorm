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
package com.alibaba.jstorm.performance.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * WordCount but teh spout does not stop, and the bolts are implemented in java.
 * This can show how fast the word count can run.
 */
public class FastWordCountTopology {
    private static Logger      LOG                             = LoggerFactory.getLogger(FastWordCountTopology.class);
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology.spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "topology.bolt.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "topology.bolt.parallel";
    
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
                "peter piper picked a peck of pickeled peppers" };
                
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
            sendingCount = 0;
            startTime = System.currentTimeMillis();
            sendNumPerNexttuple = Utils.getInt(conf.get("send.num.each.time"), 1);
            isStatEnable = Utils.getBoolean(conf.get("is.stat.enable"), false);
        }
        
        public void nextTuple() {
            int n = sendNumPerNexttuple;
            while (--n >= 0) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                _collector.emit(new Values(sentence));
            }
            updateSendTps();
        }
        

        public void ack(Object id) {
            // Ignored
        }
        

        public void fail(Object id) {
            _collector.emit(new Values(id), id);
        }
        

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
        

        public void close() {
            // TODO Auto-generated method stub
            
        }
        

        public void activate() {
            // TODO Auto-generated method stub
            
        }
        

        public void deactivate() {
            // TODO Auto-generated method stub
            
        }
        

        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    public static class SplitSentence implements IRichBolt {
        OutputCollector collector;
        

        public void execute(Tuple tuple) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word));
            }
        }
        

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
        
  
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        

        public void cleanup() {
            // TODO Auto-generated method stub
            
        }
        
  
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    public static class WordCount implements IRichBolt {
        OutputCollector      collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();
        

        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
        }
        

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
        }
        

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            
        }
        

        public void cleanup() {
            // TODO Auto-generated method stub
            
        }
        
     
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    static boolean isLocal = true;
    static Config  conf    = null;
    
    public static void test() throws Exception{
        
        int spout_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int count_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 2);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        
        builder.setSpout("spout", new FastRandomSentenceSpout(), spout_Parallelism_hint);
        builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
    
    public static void main(String[] args) throws Exception {
        
        conf = com.alibaba.jstorm.utils.Utils.getConfig(args);
        test();
    }
}
