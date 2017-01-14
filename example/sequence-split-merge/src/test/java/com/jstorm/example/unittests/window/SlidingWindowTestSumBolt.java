package com.jstorm.example.unittests.window;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/21.
 */
public class SlidingWindowTestSumBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(SlidingWindowTestSumBolt.class);
    private int sum = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        /*
         * The inputWindow gives a view of (a) all the events in the window (b)
         * events that expired since last activation of the window (c) events
         * that newly arrived since last activation of the window
         */
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

        LOG.debug("Events in current window: " + tuplesInWindow.size());
        /*
         * Instead of iterating over all the tuples in the window to compute the
         * sum, the values for the new events are added and old events are
         * subtracted. Similar optimizations might be possible in other
         * windowing computations.
         */
        for (Tuple tuple : newTuples) {
            sum += (int) tuple.getValue(0);
        }
        for (Tuple tuple : expiredTuples) {
            sum -= (int) tuple.getValue(0);
        }
        collector.emit(new Values(sum));
        LOG.info("#sum = " + sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }
}
