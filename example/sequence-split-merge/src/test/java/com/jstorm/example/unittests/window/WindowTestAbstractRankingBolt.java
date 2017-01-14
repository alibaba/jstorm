package com.jstorm.example.unittests.window;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import org.apache.storm.starter.tools.Rankings;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/11.
 */
public abstract class WindowTestAbstractRankingBolt extends BaseBasicBolt {

    private static final long serialVersionUID                  = 4931640198501530202L;
    private static final int  DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int  DEFAULT_COUNT                     = 10;

    private final int      emitFrequencyInSeconds;
    private final int      count;
    private final Rankings rankings;

    public WindowTestAbstractRankingBolt() {
        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public WindowTestAbstractRankingBolt(int topN) {
        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public WindowTestAbstractRankingBolt(int topN, int emitFrequencyInSeconds) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException(
                    "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }
        count = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(count);
    }

    protected Rankings getRankings() {
        return rankings;
    }

    /**
     * This method functions as a template method (design pattern).
     */
    @Override
    public final void execute(Tuple tuple, BasicOutputCollector collector) {
        if (TupleUtils.isTick(tuple)) {
            getLogger().debug("Received tick tuple, triggering emit of current rankings");
            emitRankings(collector);
        } else {
            updateRankingsWithTuple(tuple);
        }
    }

    abstract void updateRankingsWithTuple(Tuple tuple);

    private void emitRankings(BasicOutputCollector collector) {
        collector.emit(new Values(rankings.copy()));
        getLogger().info("AbstractRankerBolt Rankings: " + rankings);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rankings"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    abstract Logger getLogger();
}
