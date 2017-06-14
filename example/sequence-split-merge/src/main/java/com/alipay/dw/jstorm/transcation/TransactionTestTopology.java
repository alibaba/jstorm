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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.transactional.bolt.ITransactionBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.ITransactionStatefulBoltExecutor;
import com.alibaba.jstorm.transactional.spout.ITransactionSpoutExecutor;
import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.FastRandomSentenceSpout;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.SplitSentence;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology.WordCount;

public class TransactionTestTopology {
    private static Logger LOG = LoggerFactory.getLogger(TransactionTestTopology.class);
    
    public final static String SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String COUNT_PARALLELISM_HINT = "count.parallel";
    
    public static class BasicTxSpout extends FastRandomSentenceSpout implements ITransactionSpoutExecutor {
        private int tupleNumPerBatch;

        @Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            super.open(conf, context, collector);
            tupleNumPerBatch = JStormUtils.parseInt(conf.get("tuple.num.per.batch"), 50000);
		}
        
        @Override
        public void nextTuple() {
            int n = tupleNumPerBatch;
            while (--n >= 0) {
                collector.emit(new Values(CHOICES[index]));
                index = (++index) % CHOICES.length;
            }
            collector.emitBarrier();
            printSendTps(tupleNumPerBatch);
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
    
    public static class ScheduleTxSpout extends FastRandomSentenceSpout implements ITransactionSpoutExecutor {
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
    
    public static class TxSplitSentence extends SplitSentence implements ITransactionBoltExecutor {
        
    }
    
    public static class TxWordCount extends WordCount implements ITransactionStatefulBoltExecutor {
        @Override
        public Object finishBatch(long batchId) {
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
    
    static boolean      isLocal     = true;
    static List<String> hosts;
    static boolean      spoutSingle = true;
    static Config       conf        = JStormHelper.getConfig(null);
    
    public static void test() throws Exception {
        TransactionTopologyBuilder builder = new TransactionTopologyBuilder();
        if (isLocal) {
            conf.put("tuple.num.per.batch", 5);
            conf.put("transaction.scheduler.spout", false);
            conf.put("transaction.exactly.cache.type", "default");
        }

        int spoutParallelism = JStormUtils.parseInt(conf.get(SPOUT_PARALLELISM_HINT), 1);
        int splitParallelism = JStormUtils.parseInt(conf.get(SPLIT_PARALLELISM_HINT), 2);
        int countParallelism = JStormUtils.parseInt(conf.get(COUNT_PARALLELISM_HINT), 2);

        boolean isScheduleSpout = JStormUtils.parseBoolean(conf.get("transaction.scheduler.spout"), true);
        if (isScheduleSpout)
            // Generate batch by configured time. "transaction.schedule.batch.delay.ms: 1000 # 1sec"
            builder.setSpout("spout", new ScheduleTxSpout(), spoutParallelism);
        else
            // Generate batch by user when calling emitBarrier
            builder.setSpout("spout", new BasicTxSpout(), spoutParallelism, false);
        builder.setBolt("split", new TxSplitSentence(), splitParallelism).localOrShuffleGrouping("spout");
        builder.setBolt("count", new TxWordCount(), countParallelism).fieldsGrouping("split", new Fields("word"));

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
    
    public static void main(String[] args) throws Exception {
        isLocal = false;
        conf = JStormHelper.getConfig(args);
        test();
    }
    
}
