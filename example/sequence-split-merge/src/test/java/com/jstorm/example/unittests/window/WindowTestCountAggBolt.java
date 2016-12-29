package com.jstorm.example.unittests.window;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/22.
 */
public class WindowTestCountAggBolt extends BaseRichBolt {
    // Mapping of key->upstreamBolt->count
    private Map<Object, Map<Integer, Long>> counts = new HashMap<Object, Map<Integer, Long>>();
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Object obj = input.getValue(0);
        long count = input.getLong(1);
        int source = input.getSourceTask();

        System.out.println("### obj = " + obj + " count = " + count + " src = " +source);

        Map<Integer, Long> subCounts = counts.get(obj);
        if (subCounts == null) {
            subCounts = new HashMap<Integer, Long>();
            counts.put(obj, subCounts);
        }
        // Update the current count for this object
        subCounts.put(source, count);
        // Output the sum of all the known counts so for this key
        long sum = 0;
        for (Long val : subCounts.values()) {
            sum += val;
        }
        collector.emit(new Values(obj, sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count"));
    }
}
