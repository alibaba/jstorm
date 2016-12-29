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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;

import backtype.storm.Config;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This is a basic example of a transactional topology. It keeps a count of the
 * number of tuples seen so far in a database. The source of data and the
 * databases are mocked out as in memory maps for demonstration purposes. This
 * class is defined in depth on the wiki at
 * https://github.com/nathanmarz/storm/wiki/Transactional-topologies
 */
public class TransactionalGlobalCount {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalGlobalCount.class);
    
    public static final int                              PARTITION_TAKE_PER_BATCH = 3;
    public static final Map<Integer, List<List<Object>>> DATA                     = new HashMap<Integer, List<List<Object>>>() {
        {
            put(0, new ArrayList<List<Object>>() {
                {
                    add(new Values("cat"));
                    add(new Values("dog"));
                    add(new Values("chicken"));
                    add(new Values("cat"));
                    add(new Values("dog"));
                    add(new Values("apple"));
                }
            });
            put(1, new ArrayList<List<Object>>() {
                {
                    add(new Values("cat"));
                    add(new Values("dog"));
                    add(new Values("apple"));
                    add(new Values("banana"));
                }
            });
            put(2, new ArrayList<List<Object>>() {
                {
                    add(new Values("cat"));
                    add(new Values("cat"));
                    add(new Values("cat"));
                    add(new Values("cat"));
                    add(new Values("cat"));
                    add(new Values("dog"));
                    add(new Values("dog"));
                    add(new Values("dog"));
                    add(new Values("dog"));
                }
            });
        }
    };
    
    public static class Value {
        int        count = 0;
        BigInteger txid;
    }
    
    public static Map<String, Value> DATABASE         = new HashMap<String, Value>();
    public static final String       GLOBAL_COUNT_KEY = "GLOBAL-COUNT";
    
    public static class BatchCount extends BaseBatchBolt {
        Object               _id;
        BatchOutputCollector _collector;
        
        int _count = 0;
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }
        
        @Override
        public void execute(Tuple tuple) {
            _count++;
            LOG.info("---BatchCount.execute(), _count=" + _count);
        }
        
        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
            LOG.info("---BatchCount.finishBatch(), _id=" + _id + ", _count=" + _count);
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "count"));
        }
    }
    
    public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
        TransactionAttempt   _attempt;
        BatchOutputCollector _collector;
        
        int _sum = 0;
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
                TransactionAttempt attempt) {
            _collector = collector;
            _attempt = attempt;
        }
        
        @Override
        public void execute(Tuple tuple) {
            _sum += tuple.getInteger(1);
            LOG.info("---UpdateGlobalCount.execute(), _sum=" + _sum);
        }
        
        @Override
        public void finishBatch() {
            Value val = DATABASE.get(GLOBAL_COUNT_KEY);
            Value newval;
            if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
                newval = new Value();
                newval.txid = _attempt.getTransactionId();
                if (val == null) {
                    newval.count = _sum;
                } else {
                    newval.count = _sum + val.count;
                }
                DATABASE.put(GLOBAL_COUNT_KEY, newval);
            } else {
                newval = val;
            }
            _collector.emit(new Values(_attempt, newval.count));
            LOG.info("---UpdateGlobalCount.finishBatch(), _attempt=" + _attempt + ", newval=(" + newval.txid + ","
                    + newval.count + ")");
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "sum"));
        }
    }
    
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"),
                PARTITION_TAKE_PER_BATCH);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 1);
        builder.setBolt("partial-count", new BatchCount(), 2).noneGrouping("spout");
        builder.setBolt("sum", new UpdateGlobalCount(), 1).globalGrouping("partial-count");
        
        conf.setDebug(true);
        conf.setMaxSpoutPending(3);
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        try {
            JStormHelper.runTopology(builder.buildTopology(), topologyName, conf, 60,
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
