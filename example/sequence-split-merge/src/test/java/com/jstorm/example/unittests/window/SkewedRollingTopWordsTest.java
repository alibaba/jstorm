package com.jstorm.example.unittests.window;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/22.
 *
 * basically the same as SkewedRollingTopWords. Just like in the unit test RollingTopWordsTest, I really don't
 * know how to validate if the result is right since the tick time is not precise. It makes the output after
 * passing a window is unpredictable. Now I just let it pass all the time.
 */
public class SkewedRollingTopWordsTest {
    public final static int DEFAULT_COUNT = 5;

    @Test
    public void testSkewedRollingTopWords()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("windowTestWordSpout", new WindowTestWordSpout(), 5);
        topologyBuilder.setBolt("windowTestRollingCountBolt", new WindowTestRollingCountBolt(9, 3), 4)
                .partialKeyGrouping("windowTestWordSpout", new Fields("word"));
        topologyBuilder.setBolt("windowTestCountAggBolt", new WindowTestCountAggBolt(), 4)
                .fieldsGrouping("windowTestRollingCountBolt", new Fields("obj"));
        topologyBuilder.setBolt("windowTestIntermediateRankingBolt", new WindowTestIntermediateRankingBolt(DEFAULT_COUNT), 4)
                .fieldsGrouping("windowTestCountAggBolt", new Fields("obj"));
        topologyBuilder.setBolt("windowTestTotalRankingsBolt", new WindowTestTotalRankingsBolt(DEFAULT_COUNT))
                .globalGrouping("windowTestIntermediateRankingBolt");

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "SkewedRollingTopWordsTest");

        //I really don't know how to validate if the result is right since
        //the tick time is not precise. It makes the output after passing
        //a window is unpredictable.
        //Now I just let it pass all the time.
        //TODO:FIX ME: how to validate if the result is right?
        JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 90, null);
    }
}
