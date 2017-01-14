package com.alipay.dw.jstorm.example.performance.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
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

import com.alibaba.jstorm.utils.JStormUtils;

public class PerformanceTestTopology {
    private static Logger LOG = LoggerFactory.getLogger(PerformanceTestTopology.class);
    
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT  = "bolt.parallel";
    
    public static class TestSpout implements IRichSpout {
        SpoutOutputCollector collector;
        
        long    count;
        boolean isAckRequired;
        int     sendNumEachTime;
        long    startTimeStamp;
        
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.count = 0l;
            this.isAckRequired = JStormUtils.parseBoolean(conf.get("is.ack.required"), false);
            this.sendNumEachTime = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
            this.startTimeStamp = System.currentTimeMillis();
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
        public void nextTuple() {
            int sendNum = sendNumEachTime;
            while (--sendNum >= 0) {
                ++count;
                collector.emit(new Values(count), count);
            }
        }
        
        @Override
        public void ack(Object msgId) {
        
        }
        
        @Override
        public void fail(Object msgId) {
            Integer val = (Integer) msgId;
            collector.emit(new Values(val), val);
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Value"));
            
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static class TestBolt implements IRichBolt {
        OutputCollector collector;
        
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            
        }
        
        @Override
        public void execute(Tuple input) {
            long count = input.getLong(0);
            collector.ack(input);
        }
        
        @Override
        public void cleanup() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Value"));
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static void SetRemoteTopology()
            throws AlreadyAliveException, InvalidTopologyException, TopologyAssignException {
        String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
        if (streamName == null) {
            String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
            streamName = className[className.length - 1];
        }
        
        TopologyBuilder builder = new TopologyBuilder();
        
        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int bolt_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);
        builder.setSpout("spout", new TestSpout(), spout_Parallelism_hint);
        
        BoltDeclarer boltDeclarer = builder.setBolt("bolt", new TestBolt(), bolt_Parallelism_hint);
        // localFirstGrouping is only for jstorm
        // boltDeclarer.localFirstGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
        boltDeclarer.shuffleGrouping("spout");
        // .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        StormSubmitter.submitTopology(streamName, conf, builder.createTopology());
        
    }
    
    private static Map conf = new HashMap<Object, Object>();
    
    public static void LoadProperty(String prop) {
        Properties properties = new Properties();
        
        try {
            InputStream stream = new FileInputStream(prop);
            properties.load(stream);
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + prop);
        } catch (Exception e1) {
            e1.printStackTrace();
            
            return;
        }
        
        conf.putAll(properties);
    }
    
    public static void LoadYaml(String confPath) {
        
        Yaml yaml = new Yaml();
        
        try {
            InputStream stream = new FileInputStream(confPath);
            
            conf = (Map) yaml.load(stream);
            if (conf == null || conf.isEmpty() == true) {
                throw new RuntimeException("Failed to read config file");
            }
            
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + confPath);
            throw new RuntimeException("No config file");
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to read config file");
        }
        
        return;
    }
    
    public static void LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            LoadYaml(arg);
        } else {
            LoadProperty(arg);
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Please input configuration file");
            System.exit(-1);
        }
        
        LoadConf(args[0]);
        SetRemoteTopology();
    }
    
}
