package com.jstorm.example.unittests.sequence;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.jstorm.example.unittests.utils.JStormUnitTestMetricValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by binyang.dby on 2016/7/8.
 *
 * basically the unit test for SequenceTopology. I just add some metrics to get some parameter
 * and make some assert.This can also be an example to show how to get metric data that user has
 * define and manage in his own component.
 *
 * @Test pass at 2016/7/19
 */
public class SequenceTopologyTest
{
    private static Logger LOG = LoggerFactory.getLogger(SequenceTopologyTest.class);

    public final static int SPOUT_PARALLELISM_HINT = 1;
    public final static int BOLT_PARALLELISM_HINT = 2;
    public final static long SPOUT_MAX_SEND_NUM = 100000;

    @Test
    public void testSequenceTopology()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME, new SequenceTestSpout(), SPOUT_PARALLELISM_HINT);

        topologyBuilder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME, new SequenceTestSplitRecord(), BOLT_PARALLELISM_HINT)
                .localOrShuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);

        topologyBuilder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new SequenceTestPairCount(), BOLT_PARALLELISM_HINT)
                .shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, SequenceTopologyDef.TRADE_STREAM_ID);

        topologyBuilder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new SequenceTestPairCount(), BOLT_PARALLELISM_HINT)
                .shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, SequenceTopologyDef.CUSTOMER_STREAM_ID);

        topologyBuilder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME, new SequenceTestMergeRecord(), BOLT_PARALLELISM_HINT)
                .fieldsGrouping(SequenceTopologyDef.TRADE_BOLT_NAME, new Fields("ID"))
                .fieldsGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new Fields("ID"));

        topologyBuilder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME, new SequenceTestTotalCount(), BOLT_PARALLELISM_HINT)
                .noneGrouping(SequenceTopologyDef.MERGE_BOLT_NAME);

        Map conf = new HashMap();                               //use config in detail.yaml
//        Config.setFallBackOnJavaSerialization(conf, true);      //fall.back.on.java.serialization: true
//                                                                //enable.split: true
//        Config.registerSerialization(conf, TradeCustomer.class, TradeCustomerSerializer.class);
//        Config.registerSerialization(conf, Pair.class, PairSerializer.class);
        Config.setNumAckers(conf, 1);
        Config.setNumWorkers(conf, 3);
        conf.put("spout.max.sending.num", SPOUT_MAX_SEND_NUM);  //set a limit for the spout to get a precise
                                                                //number to make sure the topology works well.
        conf.put(Config.TOPOLOGY_NAME, "SequenceTopologyTest");

        //the following is just for the JStormUnitTestMetricValidator to pick the metric data
        //from all the metrics.If you are not using JStormUnitTestMetricValidator, it is useless.
        //The first element is the key that register in the metric, the second one is the key
        //map with the metric value as a parameter in the callback function validateMetrics().
        Set<String> userDefineMetrics = new HashSet<String>();
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_SPOUT_EMIT);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_SPOUT_SUCCESS);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_SPOUT_FAIL);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_SPOUT_TRADE_SUM);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_SPOUT_CUSTOMER_SUM);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_SPLIT_EMIT);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_PAIR_TRADE_EMIT);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_PAIR_CUSTOMER_EMIT);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_MERGE_EMIT);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_TOTAL_EXECUTE);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_TOTAL_TRADE_SUM);
        userDefineMetrics.add(SequenceTestMetricsDef.METRIC_TOTAL_CUSTOMER_SUM);


        JStormUnitTestMetricValidator validator =  new JStormUnitTestMetricValidator(userDefineMetrics) {
            @Override
            public boolean validateMetrics(Map<String, Double> metrics)
            {
                for(Map.Entry<String, Double> entry : metrics.entrySet())
                    LOG.info("user define metric Key = " + entry.getKey() + " Value = " + entry.getValue());

                int spoutEmit = (int)(metrics.get(SequenceTestMetricsDef.METRIC_SPOUT_EMIT)).doubleValue();
                int spoutSuccess = (int)(metrics.get(SequenceTestMetricsDef.METRIC_SPOUT_SUCCESS)).doubleValue();
                int spoutFail = (int)(metrics.get(SequenceTestMetricsDef.METRIC_SPOUT_FAIL)).doubleValue();
                long spoutTradeSum = (long)(metrics.get(SequenceTestMetricsDef.METRIC_SPOUT_TRADE_SUM)).doubleValue();
                long spoutCustomerSum = (long)(metrics.get(SequenceTestMetricsDef.METRIC_SPOUT_CUSTOMER_SUM)).doubleValue();

                int splitEmit = (int)(metrics.get(SequenceTestMetricsDef.METRIC_SPLIT_EMIT)).doubleValue();
                int pairTradeEmit = (int)(metrics.get(SequenceTestMetricsDef.METRIC_PAIR_TRADE_EMIT)).doubleValue();
                int pairCustomerEmit = (int)(metrics.get(SequenceTestMetricsDef.METRIC_PAIR_CUSTOMER_EMIT)).doubleValue();
                int mergeEmit = (int)(metrics.get(SequenceTestMetricsDef.METRIC_MERGE_EMIT)).doubleValue();

                int totalExecute = (int)(metrics.get(SequenceTestMetricsDef.METRIC_TOTAL_EXECUTE)).doubleValue();
                long totalTradeSum = (long)(metrics.get(SequenceTestMetricsDef.METRIC_TOTAL_TRADE_SUM)).doubleValue();
                long totalCustomerSum = (long)(metrics.get(SequenceTestMetricsDef.METRIC_TOTAL_CUSTOMER_SUM)).doubleValue();

                assertEquals(SPOUT_MAX_SEND_NUM, spoutEmit);
                assertEquals(spoutEmit, spoutSuccess);
                assertEquals(0, spoutFail);
                assertEquals(2*spoutEmit, splitEmit);
                assertEquals(splitEmit, pairTradeEmit*2);
                assertEquals(splitEmit, pairCustomerEmit*2);
                assertEquals(splitEmit, mergeEmit*2);
                assertEquals(mergeEmit, totalExecute);
                assertEquals(spoutTradeSum, totalTradeSum);
                assertEquals(spoutCustomerSum, totalCustomerSum);
                return true;
            }
        };

        //the below line time in second 150 is recommend, at least it should be more than 120 since the
        //metric data was grabbed every 60s but not so precise.
        boolean result = JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), conf, 150, validator);
        assertTrue("Topology should pass the validator", result);
    }
}
