package com.jstorm.example.unittests.window;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import org.apache.storm.starter.tools.NthLastModifiedTimeTracker;
import org.apache.storm.starter.tools.SlidingWindowCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/11.
 */
public class WindowTestRollingCountBolt extends BaseRichBolt
{
    private static Logger LOG = LoggerFactory.getLogger("WindowTestRollingCountBolt");

    private SlidingWindowCounter<Object> counter;
    private int windowLengthInSeconds;
    private int emitFrequencyInSeconds;
    private int slots;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public WindowTestRollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds)
    {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;

        if(windowLengthInSeconds % emitFrequencyInSeconds != 0)
            throw new IllegalArgumentException("WindowLengthInSeconds should be times of EmitFrequencyInSeconds!");
        this.slots = windowLengthInSeconds/emitFrequencyInSeconds;
        this.counter = new SlidingWindowCounter<Object>(slots);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.lastModifiedTracker = new NthLastModifiedTimeTracker(slots);
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            System.out.println("### tuple = isTick" );
            emitCurrentWindowCounts();
        } else {
            countObjAndAck(input);
        }
    }

    private void emitCurrentWindowCounts() {
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            //LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        emit(counts, actualWindowLengthInSeconds);
    }

    private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
        }
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);
        counter.incrementCount(obj);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.info("%%%% getComponentConfiguration");
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
