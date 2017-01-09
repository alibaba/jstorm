package com.alipay.dw.jstorm.example.update.topology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IDynamicComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestBackpressure {
    private static Logger      LOG                             = LoggerFactory.getLogger(FastWordCountTopology.class);
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";
    public final static String BOLT_SLOW_DOWN = "bolt.slow.down";
    public final static int    BACKPRESSURE_NOT_START = 0;
    public final static int    BACKPRESSURE_START = 1;
    public final static int    BACKPRESSURE_CLOSE = 2;
    
    /**
     * All receive update topology component must implement IDynamicComponent interface
     * @author longda
     *
     */
    public static class FastRandomSentenceSpout implements IRichSpout,IDynamicComponent {
        SpoutOutputCollector _collector;
        Random               _rand;
        long                 sendingCount;
        long                 startTime;
        boolean              isStatEnable;
        int                  sendNumPerNexttuple;
        Map                  conf;
        boolean              isBoltSlowdown = true;

        
        private static final String[] CHOICES = { "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers" };
                
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            _collector = collector;
            _rand = new Random();
            sendingCount = 0;
            startTime = System.currentTimeMillis();
            sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
            isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), true);
        }
        
        @Override
        public void nextTuple() {
            int n = sendNumPerNexttuple;
            while (--n >= 0) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                _collector.emit(new Values(sentence));
            }
            updateSendTps();
            if (isBoltSlowdown == false) {
                //avoid to occur backpress, so slow down spout
                JStormUtils.sleepMs(1);
            }
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

        @Override
        public void update(Map conf) {
         // TODO Auto-generated method stub
            LOG.info("Receive configuration {}", conf);
            this.conf.putAll(conf);
            
            isBoltSlowdown = JStormUtils.parseBoolean(conf.get(BOLT_SLOW_DOWN), true);
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
    
    /**
     * All receive update topology component must implement IDynamicComponent interface
     * @author longda
     *
     */
    public static class WordCount implements IRichBolt, IDynamicComponent {
        OutputCollector      collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();
        boolean              isSlowdown = true;
        Map                  conf;
        
        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
            if (isSlowdown == true) {
                JStormUtils.sleepMs(100);
            }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
        }
        
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.conf = stormConf;
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

        @Override
        public void update(Map conf) {
            // TODO Auto-generated method stub
            LOG.info("Receive configuration {}", conf);
            this.conf.putAll(conf);
            
            isSlowdown = JStormUtils.parseBoolean(conf.get(BOLT_SLOW_DOWN), true);
        }
    }
    
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    
    static {
        conf.put(Config.STORM_CLUSTER_MODE, "local");
    }
    
    /**
     * Can't run 
     */
    public static void test() {
        
        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int count_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 2);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        
        builder.setSpout("spout", new FastRandomSentenceSpout(), spout_Parallelism_hint);
        builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        isLocal = JStormHelper.localMode(conf);
        
        conf.put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, true);
        conf.setNumWorkers(8);
        
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60 * 2,
                    new Validator(conf), isLocal);
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
    
    public static class Validator implements Callback {
        private Map conf;
        public Validator(Map conf) {
            this.conf = conf;
        }
        
        /**
         * 
         * @param
         * @return
         * 0  -- not occur
         * 1  -- backpressure start
         * 2  -- backpressure close
         * @throws Exception 
         */

        @Override
        public <T> Object execute(T... args) {
            /*String topologyName = (String)args[0];
            
            NimbusClientWrapper client = new NimbusClientWrapper();
            
            int backpressureStatus = 0;
            String topologyId = null;
            try {
                try {
                    client.init(conf);
                    
                    topologyId = client.getClient().getTopologyId(topologyName);
                    
                    backpressureStatus = getBackpressure(topologyId);
                    
                } catch (Exception e) {
                    Assert.fail("Fail to get backpressure status");
                }
                
                System.out.println("Begin to check whether backpress is enable or not");
                if (backpressureStatus != BACKPRESSURE_START) {
                    Assert.fail("Backpressure should start, but not.");
                }
                System.out.println("Backpress is be started");
                
                Map commandConf = new HashMap<>();
                commandConf.put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, false);
                
                try {
                    client.getClient().updateTopology(topologyName, null, JStormUtils.to_json(commandConf));
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    Assert.fail("Failed to issue update command");
                }
                
                JStormUtils.sleepMs(20000);
                
                try {
                    backpressureStatus = getBackpressure(topologyId);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    Assert.fail("Fail to get backpressure status");
                }
                
                System.out.println("Begin to check whether backpress is enable or not");
                if (backpressureStatus != BACKPRESSURE_CLOSE) {
                    Assert.fail("Backpressure should be close, but not.");
                }
                System.out.println("Backpress is close.");
                
                commandConf = new HashMap<>();
                commandConf.put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, true);
                
                try {
                    client.getClient().updateTopology(topologyName, null, JStormUtils.to_json(commandConf));
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    Assert.fail("Failed to issue update command");
                }
                
                JStormUtils.sleepMs(20000);
                
                try {
                    backpressureStatus = getBackpressure(topologyId);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    Assert.fail("Fail to get backpressure status");
                }
                
                System.out.println("Begin to check whether backpress is enable or not");
                if (backpressureStatus != BACKPRESSURE_START) {
                    Assert.fail("Backpressure should be startted, but not.");
                }
                System.out.println("Backpress is started.");
                
                commandConf = new HashMap<>();
                commandConf.put(BOLT_SLOW_DOWN, false);
                
                try {
                    client.getClient().updateTopology(topologyName, null, JStormUtils.to_json(commandConf));
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    Assert.fail("Failed to issue update command");
                }
                
                JStormUtils.sleepMs(4 * 60 * 1000);
                
                try {
                    backpressureStatus = getBackpressure(topologyId);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    Assert.fail("Fail to get backpressure status");
                }
                
                System.out.println("Begin to check whether backpress is enable or not");
                if (backpressureStatus != BACKPRESSURE_CLOSE) {
                    Assert.fail("Backpressure should be close, but not.");
                }
                System.out.println("Backpress is closed.");
                
                Callback callback = new JStormHelper.CheckAckedFail(conf);
                callback.execute(topologyName);
            } finally {
                client.cleanup();
            }*/
            
            return null;
        }
    }
}
