package com.jstorm.example.unittests.tick;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.jstorm.example.unittests.utils.JStormUnitTestMetricValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import com.jstorm.example.unittests.utils.JStormUnitTestValidator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.*;

/**
 * Created by binyang.dby on 2016/7/21.
 *
 * This test is to check if the tick works correctly with the cycle which
 * is set by the user.
 */
public class TickTupleTest{
    //note that it is a cycle but not a frequency, it means you will receive a tick tuple every 2 seconds
    public final static int TICK_TUPLE_CYCLE = 2;
    public final static int TICK_TUPLE_BOLT_PARALLELISM = 5;
    private static Logger LOG = LoggerFactory.getLogger(TickTupleTest.class);

    @Test
    public void testTickTuple()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //the spout is useless in this case, all the methods are empty
        topologyBuilder.setSpout("spout", new TickTupleTestSpout());
        //note that there is no grouping here, the bolt should not receive any tuple from spout
        //I increase the parallelism of the bolt to check if it is correct when we have a
        //parallelism greater than 1.
        topologyBuilder.setBolt("bolt", new TickTupleTestBolt(), TICK_TUPLE_BOLT_PARALLELISM)
                .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_TUPLE_CYCLE);

        Set<String> userDefineMetrics = new HashSet<String>();
        userDefineMetrics.add("TickTupleTest.TickMeter");
        userDefineMetrics.add("TickTupleTest.NonTickCounter");

        JStormUnitTestValidator validator = new JStormUnitTestMetricValidator(userDefineMetrics) {
            @Override
            public boolean validateMetrics(Map<String, Double> metrics) {
                //there is $TICK_TUPLE_BOLT_PARALLELISM bolts, so the TickMeter need divided by $TICK_TUPLE_BOLT_PARALLELISM
                double cycle = 1 / (metrics.get("TickTupleTest.TickMeter") / TICK_TUPLE_BOLT_PARALLELISM);
                LOG.info("TickTupleTest.TickMeter = " + metrics.get("TickTupleTest.TickMeter"));
                LOG.info("Tick cycle  = " + cycle);
                assertTrue("The tick cycle should be in range of 0.8*TICK_TUPLE_CYCLE and 1.2*TICK_TUPLE_CYCLE",
                        cycle > 0.9f*TICK_TUPLE_CYCLE && cycle < 1.1f*TICK_TUPLE_CYCLE);
                assertEquals(0, (int)(metrics.get("TickTupleTest.NonTickCounter").doubleValue()));

                return true;
            }
        };

        Config config = new Config();
        config.put(Config.TOPOLOGY_NAME, "TickTupleTest");

        JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 120, validator);
    }
}
