package com.alipay.dw.jstorm.transcation;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import com.alibaba.jstorm.transactional.bolt.KvStatefulBoltExecutor;
import com.alibaba.jstorm.transactional.state.IKvState;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.starter.utils.JStormHelper;
import com.alipay.dw.jstorm.transcation.TransactionTestTopology.ScheduleTxSpout;
import com.alipay.dw.jstorm.transcation.TransactionTestTopology.TxSplitSentence;

public class TransactionStateTest {
    private static Logger LOG = LoggerFactory.getLogger(TransactionStateTest.class);

    public final static String SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String COUNT_PARALLELISM_HINT = "count.parallel";

    public static class RocksDbCount extends KvStatefulBoltExecutor<String, Integer> {
        OutputCollector      collector;
        Map<String, Integer> cacheCounts;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector, IKvState<String, Integer> state) {
            this.collector = collector;
            this.cacheCounts = new HashMap<String, Integer>();
        }
        
        @Override
        public void execute(Tuple input, IKvState<String, Integer> state) {
            String word = input.getString(0);

            Integer count = state.get(word);
            if (count == null)
                count = 0;
            state.put(word, ++count);

            // For performance purpose, you can cache the value first, and then put them into state instance when checkpoint
            // get value from cache. If not found, try to get value from state instance
            /*Integer count = cacheCounts.get(word);
            if (count == null) {
                count = state.get(word);
                if (count == null)
                    count = 0;
            }
            cacheCounts.put(word, ++count);*/
        }

        @Override 
        public void checkpoint(long batchId, IKvState<String, Integer> state) {
            // For performance purpose, we can write the whole cache when checkpoint
            /*state.putBatch(cacheCounts);
            cacheCounts.clear();*/
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
    }

    public static void main(String[] args) throws Exception {
        Map conf = JStormHelper.getConfig(args);
        int spoutParallelism = JStormUtils.parseInt(conf.get(SPOUT_PARALLELISM_HINT), 1);
        int splitParallelism = JStormUtils.parseInt(conf.get(SPLIT_PARALLELISM_HINT), 2);
        int countParallelism = JStormUtils.parseInt(conf.get(COUNT_PARALLELISM_HINT), 2);

        TransactionTopologyBuilder builder = new TransactionTopologyBuilder();
        builder.setSpout("spout", new ScheduleTxSpout(), spoutParallelism);
        builder.setBolt("split", new TxSplitSentence(), splitParallelism).localOrShuffleGrouping("spout");
        builder.setBolt("count", new RocksDbCount(), countParallelism).fieldsGrouping("split", new Fields("word"));
        builder.enableHdfs();

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}