package com.jstorm.example.unittests.drpc;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import com.jstorm.example.unittests.utils.JStormUnitTestDRPCValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.apache.storm.starter.BasicDRPCTopology;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/22.
 */
public class BasicDRPCTopologyTest {
    @Test
    public void testBasicDRPCTopology()
    {
        LinearDRPCTopologyBuilder topologyBuilder = new LinearDRPCTopologyBuilder("exclamation");
        topologyBuilder.addBolt(new BasicDRPCTopology.ExclaimBolt(), 3);

        Config config = new Config();
        config.put(Config.TOPOLOGY_NAME, "BasicDRPCTopologyTest");

        LocalDRPC localDRPC = new LocalDRPC();
        JStormUnitTestDRPCValidator validator = new JStormUnitTestDRPCValidator(localDRPC) {
            @Override
            public boolean validate(Map config)
            {
                String result = executeLocalDRPC("exclamation", "hello");
                assertEquals("hello!", result);

                result = executeLocalDRPC("exclamation", "goodbye");
                assertEquals("goodbye!", result);

                return true;
            }
        };

        try {
            JStormUnitTestRunner.submitTopology(topologyBuilder.createLocalTopology(localDRPC), config, 120, validator);
        }
        finally {
            localDRPC.shutdown();
        }
    }
}
