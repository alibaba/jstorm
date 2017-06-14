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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Map;
import java.util.Random;

/**
 * @author binyang.dby on 2016/7/20.
 */
public class SimpleBatchTestSpout implements IBatchSpout
{
    private Random random;
    public final static int BATCH_SIZE = 30;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        random = new Random(System.currentTimeMillis());
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        BatchId batchId = (BatchId) tuple.getValue(0);
        if(batchId.getId() > 100)
        {
            JStormUtils.sleepMs(1000);
            return;
        }

        for (int i = 0; i < BATCH_SIZE; i++) {
            long value = random.nextInt(100);
            basicOutputCollector.emit(new Values(batchId, value));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public byte[] commit(BatchId batchId) throws FailedException {
        System.out.println("SimpleBatchTestSpout #commit");
        return new byte[0];
    }

    @Override
    public void revert(BatchId batchId, byte[] bytes) {
        System.out.println("SimpleBatchTestSpout #revert");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("BatchId", "Value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
