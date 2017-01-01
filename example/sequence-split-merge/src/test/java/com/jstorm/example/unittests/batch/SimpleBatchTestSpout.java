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
 * Created by binyang.dby on 2016/7/20.
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
