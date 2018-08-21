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
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.window.BaseWindowedBolt;
import com.alibaba.jstorm.window.Time;
import com.alibaba.jstorm.window.TimeWindow;
import com.alibaba.starter.utils.JStormHelper;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WordCount but the spout does not stop, and the bolts are implemented in java.
 * This can show how fast the word count can run.
 */
public class FastWordCountTopNWindowTopology {
    private static Logger LOG = LoggerFactory.getLogger(FastWordCountTopNWindowTopology.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";

    public static class FastRandomSentenceSpout implements IRichSpout {
        SpoutOutputCollector collector;
        Random _rand;
        long startTime;
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
            this.collector = collector;
            _rand = new Random();
            startTime = System.currentTimeMillis();
        }

        @Override
        public void nextTuple() {
            String sentence = CHOICES[index++];
            if (index >= CHOICES.length) {
                index = 0;
            }
            collector.emit(new Values(sentence));
            JStormUtils.sleepMs(50);
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
            collector.emit(new Values(id), id);
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
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseWindowedBolt<Tuple> {
        OutputCollector collector;
        int n;

        WordCount(int n) {
            this.n = n;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("topn_part"));
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
            HashMap<String, Integer> counts = (HashMap<String, Integer>) state;
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            counts.put(word, ++count);
        }

        @Override
        public void purgeWindow(Object state, TimeWindow window) {
            LOG.info("purging window: {}", window);
            final HashMap<String, Integer> counts = (HashMap<String, Integer>) state;
            List<String> keys = Lists.newArrayList(counts.keySet());
            Collections.sort(keys, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return -counts.get(o1).compareTo(counts.get(o2));
                }
            });

            List<Object> pairs = Lists.newArrayListWithCapacity(n);
            for (int i = 0; i < n; i++) {
                pairs.add(new Pair<>(keys.get(i), counts.get(keys.get(i))));
            }
            collector.emit(pairs);
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class MergeTopN extends BaseWindowedBolt<Tuple> {
        private final int n;
        OutputCollector collector;

        MergeTopN(int n) {
            this.n = n;
        }

        @Override
        public void execute(Tuple tuple, Object state, TimeWindow window) {
            LOG.info("executing on window:{}", window);
            Map<String, Integer> counts = (Map<String, Integer>) state;
            List<Object> partialWordCounts = tuple.getValues();
            for (Object partialWordCount : partialWordCounts) {
                Pair<String, Integer> pair = (Pair<String, Integer>) partialWordCount;
                counts.put(pair.getFirst(), pair.getSecond());
            }
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
        }

        @Override
        public Object initWindowState(TimeWindow window) {
            return new HashMap<>();
        }

        @Override
        public void purgeWindow(Object state, TimeWindow window) {
            final HashMap<String, Integer> counts = (HashMap<String, Integer>) state;
            List<String> keys = Lists.newArrayList(counts.keySet());
            Collections.sort(keys, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return -counts.get(o1).compareTo(counts.get(o2));
                }
            });

            LOG.info("\npurging window");
            LOG.info("==========================================");
            for (int i = 0; i < n; i++) {
                LOG.info("word:{}, count:{}", keys.get(i), counts.get(keys.get(i)));
            }
            LOG.info("==========================================\n");

        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }


    private static Config conf;

    public static void test() {
        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int count_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 1);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FastRandomSentenceSpout(), spout_Parallelism_hint);
        builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");

        int topN = 10;
        Time win = Time.seconds(10L);
        builder.setBolt("count", new WordCount(topN)
                        .timeWindow(win)
                        .withStateSize(Time.seconds(120L)),
                count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("merge",
                new MergeTopN(topN).timeWindow(win), 1).allGrouping("count");

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];

        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        conf = JStormHelper.getConfig(args);
        test();
    }
}
