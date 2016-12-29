package com.alipay.dw.jstorm.transcation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.transactional.bolt.ITransactionBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.ITransactionStatefulBoltExecutor;
import com.alibaba.jstorm.transactional.spout.ITransactionSpoutExecutor;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;

public class TransactionTestTopology {
    private static Logger LOG = LoggerFactory.getLogger(TransactionTestTopology.class);
    
    public final static String SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String COUNT_PARALLELISM_HINT = "count.parallel";
    
    public static class Spout implements ITransactionSpoutExecutor {
        SpoutOutputCollector collector;
        int                  index = 0;
        long                 sendingCount;
        long                 startTime;
        boolean              isStatEnable;
        int                  sendNumPerNexttuple;
        
        String[] CHOICES = { "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers" };
                
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            sendingCount = 0;
            startTime = System.currentTimeMillis();
            sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
            isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        }
        
        @Override
        public void close() {
        
        }
        
        @Override
        public void activate() {
        
        }
        
        @Override
        public void deactivate() {
        
        }
        
        @Override
        public void nextTuple() {
            int n = sendNumPerNexttuple;
            while (--n >= 0) {
                collector.emit(new Values(CHOICES[index]));
                index = (++index) % CHOICES.length;
            }
            updateSendTps();
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
        
        @Override
        public void ack(Object msgId) {
        
        }
        
        @Override
        public void fail(Object msgId) {
        
        }
        
        @Override
        public void initState(Object state) {
            if (state != null) {
                index = (Integer) state;
            }
            LOG.info("initState, index=" + index);
        }
        
        @Override
        public Object finishBatch() {
            return index;
        }
        
        @Override
        public void rollBack(Object state) {
            if (state != null) {
                index = (Integer) state;
            }
            LOG.info("rollback! index=" + index);
        }
        
        @Override
        public Object commit(BatchGroupId id, Object stateData) {
        	/* If any failure happened during committing, just return "fail" flag
        	 * return TransactionCommon.COMMIT_FAIL;
        	 */
            return stateData;
        }
        
        @Override
        public void ackCommit(BatchGroupId id) {
        
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
    }
    
    public static class Split implements ITransactionBoltExecutor {
        OutputCollector collector;
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        
        @Override
        public void execute(Tuple input) {
            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word));
            }
        }
        
        @Override
        public void cleanup() {
        
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    
    public static class Count implements ITransactionStatefulBoltExecutor {
        OutputCollector      collector;
        Map<String, Integer> counts;
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.counts = new HashMap<String, Integer>();
        }
        
        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
        }
        
        @Override
        public void cleanup() {
        
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
        }
        
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
        
        @Override
        public Object finishBatch() {
            return new HashMap<String, Integer>(counts);
        }
        
        @Override
        public void initState(Object state) {
            if (state != null) {
                counts = (HashMap<String, Integer>) state;
            }
            LOG.info("initState, counts=" + counts);
        }
        
        @Override
        public void rollBack(Object state) {
            if (state != null) {
                counts = (HashMap<String, Integer>) state;
            }
            LOG.info("rollBack, counts=" + counts);
        }
        
        @Override
        public Object commit(BatchGroupId id, Object stateData) {
        	/* If any failure happened during committing, just return "fail" flag
        	 * return TransactionCommon.COMMIT_FAIL;
        	 */
            return stateData;
        }
        
        @Override
        public void ackCommit(BatchGroupId id) {
        
        }
    }
    

    
    static boolean      isLocal     = true;
    static List<String> hosts;
    static boolean      spoutSingle = true;
    static Config       conf        = JStormHelper.getConfig(null);
    
    public static void test() throws Exception {
        TransactionTopologyBuilder builder = new TransactionTopologyBuilder();
        
        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = JStormUtils.parseInt(conf.get(SPLIT_PARALLELISM_HINT), 2);
        int count_Parallelism_hint = JStormUtils.parseInt(conf.get(COUNT_PARALLELISM_HINT), 2);
        builder.setSpout("spout", new Spout(), spout_Parallelism_hint);
        builder.setBolt("split", new Split(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new Count(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 120,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
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
    
}
