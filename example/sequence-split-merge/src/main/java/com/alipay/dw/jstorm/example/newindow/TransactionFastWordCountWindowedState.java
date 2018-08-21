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
package com.alipay.dw.jstorm.example.newindow;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import com.alibaba.jstorm.transactional.bolt.ITransactionBoltExecutor;
import com.alibaba.jstorm.transactional.spout.ITransactionSpoutExecutor;
import com.alibaba.jstorm.transactional.state.IKvState;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.window.BaseWindowedBolt;
import com.alibaba.jstorm.window.Time;
import com.alibaba.jstorm.window.TimeWindow;
import com.alibaba.jstorm.window.TransactionalWindowedBolt;
import com.alibaba.starter.utils.JStormHelper;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.FastRandomSentenceSpout;
import com.alipay.dw.jstorm.transcation.TransactionTestTopology.TxSplitSentence;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WordCount but teh spout does not stop, and the bolts are implemented in java.
 * This can show how fast the word count can run.
 */
public class TransactionFastWordCountWindowedState {
    private static Logger LOG = LoggerFactory.getLogger(TransactionFastWordCountWindowedState.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";

    public static class TxFastRandomSentenceSpout extends FastRandomSentenceSpout implements ITransactionSpoutExecutor {
        @Override
        public void initState(Object state) {
            if (state != null) {
                index = (Integer) state;
            }
            LOG.info("initState, index=" + index);
        }

        @Override
        public Object finishBatch(long batchId) {
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
        public Object commit(long batchId, Object stateData) {
            /* If any failure happened during committing, just return "fail" flag
             * return TransactionCommon.COMMIT_FAIL;
             */
            return stateData;
        }

        @Override
        public void ackCommit(long batchId, long ts) {

        }
    }

    public static class WordCount extends TransactionalWindowedBolt<Tuple, String, Integer> {
        private static final long serialVersionUID = 1L;

        transient OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple, IKvState<String, Integer> state, TimeWindow window) {
            String word = (String) tuple.getValue(0);
            Integer count = state.get(word);
            if (count == null)
                count = 0;
            state.put(word, ++count);
        }

        @Override
        public void purgeWindow(IKvState<String, Integer> state, TimeWindow window) {
            LOG.info("purging window: {}", window);
            LOG.info("word counts: {}", state.getBatch());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            
        }

        @Override
        public Map<java.lang.String, Object> getComponentConfiguration() {
            return null;
        }
    }

    static Config conf = JStormHelper.getConfig(null);
    static boolean isLocal = true;

    public static void test() throws Exception {
        TransactionTopologyBuilder builder = new TransactionTopologyBuilder();
        if (isLocal) {
            conf.put("tuple.num.per.batch", 100);
            conf.put("transaction.scheduler.spout", false);
            conf.put("transaction.exactly.cache.type", "default");
            conf.put("transaction.topology", true);
        }

        int spoutParallelismHint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int splitParallelismHint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int countParallelismHint = JStormUtils.parseInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 1);

        builder.setSpout("spout", new TxFastRandomSentenceSpout(), spoutParallelismHint);
        builder.setBolt("split", new TxSplitSentence(), splitParallelismHint).localOrShuffleGrouping("spout");
        WordCount wordCount = new WordCount();
        builder.setBolt("count", wordCount
                        .timeWindow(Time.seconds(60L))
                        .withTransactionStateOperator(wordCount),
                countParallelismHint).fieldsGrouping("split", new Fields("word"));
        builder.enableHdfs();

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        conf = JStormHelper.getConfig(args);
        test();
    }
}
