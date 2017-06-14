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
package com.alipay.dw.jstorm.transcation;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.starter.utils.JStormHelper;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.FastRandomSentenceSpout;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.SplitSentence;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.WordCount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchAckerTest {
    private static Logger LOG = LoggerFactory.getLogger(BatchAckerTest.class);
    
    private final static String SPOUT_PARALLELISM_HINT = "spout.parallel";
    private final static String SPLIT_PARALLELISM_HINT = "split.parallel";
    private final static String COUNT_PARALLELISM_HINT = "count.parallel";

    private static class BatchAckerSpout extends FastRandomSentenceSpout {
        protected long msgId = 0l;

        @Override
        public void nextTuple() {
            String sentence = CHOICES[index];
            collector.emit(new Values(sentence), ++msgId);
            index = (++index) % CHOICES.length;
            printSendTps(1);
        }
        
        @Override
        public void ack(Object msgId) {
        
        }
        
        @Override
        public void fail(Object msgId) {
            LOG.info("Failure tuple: msgId={}", msgId);
        }
    }

    private static class BatchAckerValueSpout extends BatchAckerSpout implements IFailValueSpout {
        @Override
        public void fail(Object msgId, List<Object> values) {
            LOG.info("Failure tuple: msgId={}, values={}", msgId, values);
        }
    }

    private static class BatchAckerSplit extends SplitSentence {        
        @Override
        public void execute(Tuple input) {
            super.execute(input);
            collector.ack(input);
        }
    }

    private static class BatchAckerCount extends WordCount {
        @Override
        public void execute(Tuple input) {
            super.execute(input);
            collector.ack(input);
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = JStormHelper.getConfig(args);
        int spoutParallelism = JStormUtils.parseInt(conf.get(SPOUT_PARALLELISM_HINT), 1);
        int splitParallelism = JStormUtils.parseInt(conf.get(SPLIT_PARALLELISM_HINT), 2);
        int countParallelism = JStormUtils.parseInt(conf.get(COUNT_PARALLELISM_HINT), 2);
        boolean isValueSpout = JStormUtils.parseBoolean(conf.get("is.value.spout"), false);

        TransactionTopologyBuilder builder = new TransactionTopologyBuilder();
        if (isValueSpout)
            builder.setSpoutWithAck("spout", new BatchAckerValueSpout(), spoutParallelism);
        else
            builder.setSpoutWithAck("spout", new BatchAckerSpout(), spoutParallelism);
        builder.setBoltWithAck("split", new BatchAckerSplit(), splitParallelism).localOrShuffleGrouping("spout");;
        builder.setBoltWithAck("count", new BatchAckerCount(), countParallelism).fieldsGrouping("split", new Fields("word"));
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}