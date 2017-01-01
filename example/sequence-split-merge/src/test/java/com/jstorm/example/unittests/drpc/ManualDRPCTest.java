package com.jstorm.example.unittests.drpc;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.topology.TopologyBuilder;
import com.jstorm.example.unittests.utils.JStormUnitTestDRPCValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import com.jstorm.example.unittests.utils.JStormUnitTestValidator;
import org.apache.storm.starter.ManualDRPC;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/26.
 */
public class ManualDRPCTest {
    @Test
    public void testManualDRPC()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        LocalDRPC localDRPC = new LocalDRPC();

        DRPCSpout spout = new DRPCSpout("exclamation", localDRPC);
        topologyBuilder.setSpout("drpc", spout);
        topologyBuilder.setBolt("exclaim", new ManualDRPC.ExclamationBolt(), 3).shuffleGrouping("drpc");
        topologyBuilder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");

        Config config = new Config();
        config.put(Config.TOPOLOGY_NAME, "ManualDRPCTest");

        JStormUnitTestValidator validator = new JStormUnitTestDRPCValidator(localDRPC) {
            @Override
            public boolean validate(Map config) {
                assertEquals("hello!!!", executeLocalDRPC("exclamation", "hello"));
                assertEquals("good bye!!!", executeLocalDRPC("exclamation", "good bye"));
                return true;
            }
        };

        try {
            JStormUnitTestRunner.submitTopology(topologyBuilder.createTopology(), config, 60, validator);
        }
        finally {
            localDRPC.shutdown();
        }
    }
}
