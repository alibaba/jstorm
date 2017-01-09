package com.jstorm.example.unittests.window;


import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseWindowedBolt;
import com.jstorm.example.unittests.utils.JStormUnitTestMetricValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import com.jstorm.example.unittests.utils.JStormUnitTestValidator;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by binyang.dby on 2016/7/21.
 *
 * basically the unit test of SlidingWindowTopology. I change the some of the spout to
 * check if the bolt do the correct processing. I calculate what the average value will
 * be in the spout and count them up. Then I count the actual average in the AvgBolt.
 * If the 2 values are the same, the test is passed.
 *
 * @Test pass at 2016/07/21
 */
public class SlidingWindowTopologyTest {
    public final static int SPOUT_LIMIT = 3000;
    public final static int SUM_BOLT_WINDOW_LENGTH = 30;
    public final static int SUM_BOLT_WINDOW_SLIDE = 10;
    public final static int AVG_BOLT_WINDOW_LENGTH = 3;

    @Test
    public void testSlidingWindowTopology()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", new SlidingWindowTestRandomSpout(SPOUT_LIMIT), 1);
        //the following bolt sums all the elements in the window. The window has length of 30 elements
        //and slide every 10 elements.
        //for example, if the spout generate 1, 2, 3, 4 ... then the SumBolt generate 55, 210, 465, 765 ...
        topologyBuilder.setBolt("sum", new SlidingWindowTestSumBolt().withWindow(new BaseWindowedBolt.Count(SUM_BOLT_WINDOW_LENGTH),
                new BaseWindowedBolt.Count(SUM_BOLT_WINDOW_SLIDE)), 1).shuffleGrouping("spout");
        //the following bolt calculate the average value of elements in the window. The window has length
        //of 3. So it generates the average of 3 elements and then wait for another 3 elements.
        topologyBuilder.setBolt("avg", new SlidingWindowTestAvgBolt().withTumblingWindow(new BaseWindowedBolt.Count(AVG_BOLT_WINDOW_LENGTH)), 1)
                .shuffleGrouping("sum");

        Set<String> userDefineMetrics = new HashSet<String>();
        userDefineMetrics.add("SlidingWindowTopologyTest.SpoutAvgSum");
        userDefineMetrics.add("SlidingWindowTopologyTest.BoltAvgSum");

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "SlidingWindowTopologyTest");

        JStormUnitTestValidator validator = new JStormUnitTestMetricValidator(userDefineMetrics) {
            @Override
            public boolean validateMetrics(Map<String, Double> metrics) {
                int spoutAvgSum = (int) metrics.get("SlidingWindowTopologyTest.SpoutAvgSum").doubleValue();
                int boltAvgSum = (int) metrics.get("SlidingWindowTopologyTest.BoltAvgSum").doubleValue();
                System.out.println(spoutAvgSum + " " + boltAvgSum);
                assertEquals(spoutAvgSum, boltAvgSum);

                return true;
            }
        };

        JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 120, validator);
    }
}
