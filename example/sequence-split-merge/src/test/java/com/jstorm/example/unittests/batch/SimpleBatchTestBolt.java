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
package com.jstorm.example.unittests.batch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/20.
 */
public class SimpleBatchTestBolt implements IBasicBolt, ICommitter
{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleBatchTestBolt.class);
    private BatchId currentBatchId;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        BatchId id = (BatchId) tuple.getValue(0);
        Long value = tuple.getLong(1);
        System.out.println("SimpleBatchTestBolt #execute id = " + id + " value = " + value);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public byte[] commit(BatchId batchId) throws FailedException {
        System.out.println("SimpleBatchTestBolt #commit");
//        LOG.info("$$$$Receive BatchId " + batchId);
//        if (currentBatchId == null) {
//            currentBatchId = batchId;
//        } else if (currentBatchId.getId() >= batchId.getId()) {
//            LOG.info("Current BatchId is " + currentBatchId + ", receive:" + batchId);
//            throw new RuntimeException();
//        }
//        currentBatchId = batchId;

        //AtomicLong counter = (AtomicLong) counters.remove(batchId);
//        if (counter == null) {
//            counter = new AtomicLong(0);
//        }

//        LOG.info("Flush " + id + "," + counter);
//        return Utils.serialize(batchId);
        return null;
    }

    @Override
    public void revert(BatchId batchId, byte[] bytes) {
        System.out.println("SimpleBatchTestBolt #revert");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("BatchId", "counters"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
