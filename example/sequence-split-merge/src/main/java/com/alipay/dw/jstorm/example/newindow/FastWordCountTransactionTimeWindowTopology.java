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
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.window.BaseWindowedBolt;
import com.alibaba.jstorm.window.Time;
import com.alibaba.jstorm.window.TimeWindow;
import com.alibaba.starter.utils.JStormHelper;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WordCount but teh spout does not stop, and the bolts are implemented in java.
 * This can show how fast the word count can run.
 */
public class FastWordCountTransactionTimeWindowTopology {
    private static Logger LOG = LoggerFactory.getLogger(FastWordCountTransactionTimeWindowTopology.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";

    public static class TxFastRandomSentenceSpout implements IRichSpout, ITransactionSpoutExecutor {
        private static final long serialVersionUID = 1L;

        transient SpoutOutputCollector _collector;
        Random _rand;
        long startTime;
        long sentNum = 0;
        int index = 0;

        private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
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
                "Similar to Hadoop MapReduce, JStorm computes on a DAG (directed acyclic graph).",
                "Different from Hadoop MapReduce, a JStorm topology runs 24 * 7",
                "the very nature of its continuity abd 100% in-memory architecture ",
                "has been proved a particularly suitable solution for streaming data and real-time computation.",
                "JStorm guarantees fault-tolerance.",
                "Whenever a worker process crashes, ",
                "the scheduler embedded in the JStorm instance immediately spawns a new worker process to take the place of the failed one.",
                " The Acking framework provided by JStorm guarantees that every single piece of data will be processed at least once."};

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
            startTime = System.currentTimeMillis();
        }

        @Override
        public void nextTuple() {
            sentNum++;
            String sentence = CHOICES[index++];
            if (index >= CHOICES.length) {
                index = 0;
            }
            _collector.emit(new Values(sentence));
            JStormUtils.sleepMs(1);
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
            _collector.emit(new Values(id), id);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
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
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

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

    public static class SplitSentence implements IRichBolt, ITransactionBoltExecutor {
        private static final long serialVersionUID = 1L;

        transient OutputCollector collector;

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
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseWindowedBolt<Tuple> {
        private static final long serialVersionUID = 1L;

        transient OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void cleanup() {
        }

        @Override
        public Object initWindowState(TimeWindow window) {
            return new HashMap<>();
        }

        @Override
        public void execute(Tuple tuple, Object state, TimeWindow window) {
            Map<String, Integer> counts = (Map<String, Integer>) state;
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
        }

        @Override
        public void purgeWindow(Object state, TimeWindow window) {
            Map<String, Integer> counts = (Map<String, Integer>) state;
            System.out.println("purging window: " + window);
            System.out.println("=============================");
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                System.out.println("word: " + entry.getKey() + ", \t\tcount: " + entry.getValue());
            }
            System.out.println("=============================");
            System.out.println();
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
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
        builder.setBolt("split", new SplitSentence(), splitParallelismHint).localOrShuffleGrouping("spout");
        builder.setBolt("count", new WordCount()
                        .timeWindow(Time.seconds(1L))
                        .withStateSize(Time.hours(2)),
                countParallelismHint).fieldsGrouping("split", new Fields("word"));

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];

        JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                new JStormHelper.CheckAckedFail(conf), true);
    }

    public static void main(String[] args) throws Exception {
        conf = JStormHelper.getConfig(args);
        test();
    }
}
