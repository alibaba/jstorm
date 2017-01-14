package com.jstorm.example.unittests.trident;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/9.
 *
 * this is the basic class for BatchSpout which has a limit for unit test. The subclass just need to determine
 * how to generate a batch with a given max batch size.
 */
public abstract class BasicLimitBatchSpout implements IBatchSpout
{
    private long limit;
    private Fields fields;
    private int maxBatchSize;
    private HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
    private MetricClient metricClient;

    public BasicLimitBatchSpout(long limit, Fields fields, int maxBatchSize) {
        this.limit = limit;
        this.fields = fields;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        if(batchId > limit)                     //batchId starts from 1, batchId > limit means it has emitted ${limit} batches
        {
            try {
                Thread.sleep(1000);             //repeat sleep 1s until cluster is closed
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        List<List<Object>> batch;
        if(batches.containsKey(batchId)) {
            batch = batches.get(batchId);
        }
        else {
            batch = getBatchContent(maxBatchSize);
            this.batches.put(batchId, batch);
        }

        for(List<Object> tuple : batch) {
            collector.emit(tuple);
        }
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        metricClient = new MetricClient(context);
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }

    @Override
    public void ack(long batchId) {
        batches.remove(batchId);
    }

    @Override
    public void close() {}

    protected AsmCounter registerCounter(String name)
    {
        return metricClient.registerCounter(name);
    }

    protected abstract List<List<Object>> getBatchContent(int maxBatchSize);
}
