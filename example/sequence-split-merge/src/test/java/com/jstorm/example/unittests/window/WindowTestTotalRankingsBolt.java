package com.jstorm.example.unittests.window;

import backtype.storm.tuple.Tuple;
import org.apache.storm.starter.tools.Rankings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by binyang.dby on 2016/7/11.
 */
public class WindowTestTotalRankingsBolt extends WindowTestAbstractRankingBolt{
    private static final Logger LOG = LoggerFactory.getLogger(WindowTestTotalRankingsBolt.class);

    public WindowTestTotalRankingsBolt(int topN) {
        super(topN);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
        super.getRankings().updateWith(rankingsToBeMerged);
        super.getRankings().pruneZeroCounts();

        LOG.info("TotalRankingsBolt updateRankingsWithTuple " + getRankings());
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
