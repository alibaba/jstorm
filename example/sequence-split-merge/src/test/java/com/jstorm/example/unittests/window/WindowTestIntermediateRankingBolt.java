package com.jstorm.example.unittests.window;

import backtype.storm.tuple.Tuple;
import org.apache.storm.starter.tools.Rankable;
import org.apache.storm.starter.tools.RankableObjectWithFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by binyang.dby on 2016/7/11.
 */
public class WindowTestIntermediateRankingBolt extends WindowTestAbstractRankingBolt{
    private static final Logger LOG = LoggerFactory.getLogger(WindowTestIntermediateRankingBolt.class);

    public WindowTestIntermediateRankingBolt(int topN) {
        super(topN);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
