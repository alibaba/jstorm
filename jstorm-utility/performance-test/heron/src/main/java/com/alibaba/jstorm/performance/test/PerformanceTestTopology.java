package com.alibaba.jstorm.performance.test;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.Utils;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PerformanceTestTopology {
    private static Logger LOG = LoggerFactory.getLogger(PerformanceTestTopology.class);
    
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology.spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT  = "topology.bolt.parallel";
    
    public static class TestSpout implements IRichSpout {
        SpoutOutputCollector collector;
        
        long    count;
        boolean isAckRequired;
        int     sendNumEachTime;
        long    startTimeStamp;
        
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.count = 0l;
            this.isAckRequired = Utils.getBoolean(conf.get("is.ack.required"), false);
            this.sendNumEachTime = Utils.getInt(conf.get("send.num.each.time"), 1);
            this.startTimeStamp = System.currentTimeMillis();
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
        
        
        public void nextTuple() {
            int sendNum = sendNumEachTime;
            while (--sendNum >= 0) {
                ++count;
                collector.emit(new Values(count), count);
            }
        }
        
        
        public void ack(Object msgId) {
        
        }
        
        
        public void fail(Object msgId) {
            Integer val = (Integer) msgId;
            collector.emit(new Values(val), val);
        }
        
        
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Value"));
            
        }
        
        
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static class TestBolt implements IRichBolt {
        OutputCollector collector;
        
        
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            
        }
        
        
        public void execute(Tuple input) {
            long count = input.getLong(0);
            collector.ack(input);
        }
        
        
        public void cleanup() {
            // TODO Auto-generated method stub
            
        }
        
        
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Value"));
        }
        
        
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static void SetRemoteTopology()
            throws Exception {
        String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
        if (streamName == null) {
            String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
            streamName = className[className.length - 1];
        }
        
        TopologyBuilder builder = new TopologyBuilder();
        
        int spout_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int bolt_Parallelism_hint = Utils.getInt(conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);
        builder.setSpout("spout", new TestSpout(), spout_Parallelism_hint);
        
        BoltDeclarer boltDeclarer = builder.setBolt("bolt", new TestBolt(), bolt_Parallelism_hint);
        // localFirstGrouping is only for jstorm
        // boltDeclarer.localFirstGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
        boltDeclarer.shuffleGrouping("spout");
        // .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        
        StormSubmitter.submitTopology(streamName, conf, builder.createTopology());
        
    }
    
    private static Map conf = new HashMap<Object, Object>();
    
    
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Please input configuration file");
            System.exit(-1);
        }
        
        conf = com.alibaba.jstorm.utils.Utils.LoadConf(args[0]);
        SetRemoteTopology();
    }
    
}
