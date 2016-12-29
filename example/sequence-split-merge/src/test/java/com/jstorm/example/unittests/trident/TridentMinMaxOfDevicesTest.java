package com.jstorm.example.unittests.trident;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by binyang.dby on 2016/7/9.
 *
 * This is the unit test version of TridentMinMaxOfDevicesTopology.
 * The spout generate 2
 */
public class TridentMinMaxOfDevicesTest
{
    public final static int SPOUT_BATCH_SIZE = 10;

    @Test
    public void testTridentMinMaxOfDevices()
    {
        Fields fields = new Fields("device-id", "count");
        List<Values> content = new ArrayList<Values>();
        for(int i=0; i<SPOUT_BATCH_SIZE; i++)
            content.add(new Values(i+1));
        ShuffleValuesBatchSpout spout = new ShuffleValuesBatchSpout(fields, content, content);
        TridentTopology tridentTopology = new TridentTopology();
        Stream stream = tridentTopology.newStream("device-gen-spout", spout)
                .each(fields, new Debug("#### devices"));
        stream.minBy("device-id").each(fields, new AssertMinDebug());
        stream.maxBy("count").each(fields, new AssertMaxDebug());

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "TridentMinMaxOfDevicesTest");

        //the test can pass if the 2 AssertDebug pass throughout the test
        JStormUnitTestRunner.submitTopology(tridentTopology.build(), config, 120, null);
    }

    private static class AssertMinDebug extends Debug
    {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            assertEquals(1, tuple.getInteger(0).intValue());
            return true;
        }
    }

    private static class AssertMaxDebug extends Debug
    {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            assertEquals(SPOUT_BATCH_SIZE, tuple.getInteger(1).intValue());
            return true;
        }
    }
}
