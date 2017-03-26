package com.jstorm.example.unittests.batch;

import backtype.storm.Config;
import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;

/**
 * Created by binyang.dby on 2016/7/20.
 */
public class SimpleBatchTopologyTest
{
    @Test
    public void testSimpleBatchTopology() {
        BatchTopologyBuilder batchTopologyBuilder = new BatchTopologyBuilder("SimpleBatchTopology");
        batchTopologyBuilder.setSpout("batchSpout", new SimpleBatchTestSpout(), 1);
        batchTopologyBuilder.setBolt("batchBolt", new SimpleBatchTestBolt(), 2).shuffleGrouping("batchSpout");

        Config config = new Config();
        config.put(Config.TOPOLOGY_NAME, "SimpleBatchTopologyTest");
        config.setMaxSpoutPending(1);

        JStormUnitTestRunner.submitTopology(batchTopologyBuilder.getTopologyBuilder().createTopology(), config, 120, null);
    }
}
