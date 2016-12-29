package com.jstorm.example.unittests.order;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.daemon.worker.JStormDebugger;
import com.jstorm.example.unittests.utils.JStormUnitTestMetricValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author binyang.dby
 *
 * @Test pass at 2016/7/19
 */
public class InOrderDeliveryTest {
    private static Logger LOG = LoggerFactory.getLogger(InOrderDeliveryTest.class);
    public final static int SPOUT_PARALLELISM_HINT = 8;
    public final static int BOLT_PARALLELISM_HINT = 8;
    public final static long SPOUT_MAX_SEND_NUM = 1000000;

    @Test
    public void testInOrderDelivery() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", new InOrderTestSpout(SPOUT_MAX_SEND_NUM), SPOUT_PARALLELISM_HINT);
        //note that here we use fieldsGrouping, so that the tuples with the same "c1" will be sent to the same bolt.
        //as a result, even though we have BOLT_PARALLELISM_HINT bolts, each bolt maintains a order of tuples from
        //spouts that it can receive.
        topologyBuilder.setBolt("bolt", new InOrderTestBolt(), BOLT_PARALLELISM_HINT).fieldsGrouping("spout", new Fields("c1"));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "InOrderDeliveryTest");
        config.put("topology.debug.metric.names", "emit,success,fail");
        config.put("topology.debug", false);
        config.put("topology.enable.metric.debug", true);

        //the following is just for the JStormUnitTestMetricValidator to pick the metric data
        //from all the metrics.If you are not using JStormUnitTestMetricValidator, it is useless.
        //The element is the key map with the metric value as a parameter in the callback
        //function validateMetrics().
        Set<String> userDefineMetrics = new HashSet<String>();
        userDefineMetrics.add(InOrderTestMetricsDef.METRIC_SPOUT_EMIT);
        userDefineMetrics.add(InOrderTestMetricsDef.METRIC_BOLT_SUCCESS);
        userDefineMetrics.add(InOrderTestMetricsDef.METRIC_BOLT_FAIL);

        JStormUnitTestMetricValidator validator = new JStormUnitTestMetricValidator(userDefineMetrics) {
            @Override
            public boolean validateMetrics(Map<String, Double> metrics) {
                int spoutEmit = (int) metrics.get(InOrderTestMetricsDef.METRIC_SPOUT_EMIT).doubleValue();
                int boltSuccess = (int) metrics.get(InOrderTestMetricsDef.METRIC_BOLT_SUCCESS).doubleValue();
                int boltFail = (int) metrics.get(InOrderTestMetricsDef.METRIC_BOLT_FAIL).doubleValue();

                LOG.info("validateMetrics: " + "spout emit = " + spoutEmit + " bolt success = " + boltSuccess);
                assertEquals(SPOUT_MAX_SEND_NUM * SPOUT_PARALLELISM_HINT, spoutEmit);
                assertEquals(spoutEmit, boltSuccess);       //all tuples should be in order
                assertEquals(0, boltFail);

                return true;
            }
        };

        JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 120, validator);
    }
}
