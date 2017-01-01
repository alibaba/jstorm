package com.jstorm.example.unittests.performance;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/11.
 *
 * @Test pass at 2016/7/19
 */
public class FastWordCountTest {
    @Test
    public void testFastWordCount()
    {
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 1;
        int count_Parallelism_hint = 2;

        TopologyBuilder builder = new TopologyBuilder();

        boolean isLocalShuffle = false;

        builder.setSpout("spout", new FastWordCountTopology.FastRandomSentenceSpout(), spout_Parallelism_hint);
        if (isLocalShuffle)
            builder.setBolt("split", new FastWordCountTopology.SplitSentence(), split_Parallelism_hint).localFirstGrouping("spout");
        else
            builder.setBolt("split", new FastWordCountTopology.SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("count", new FastWordCountTopology.WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "FastWordCountTest");

        JStormUnitTestRunner.submitTopology(builder.createTopology(), config, 60, null);
    }
}
