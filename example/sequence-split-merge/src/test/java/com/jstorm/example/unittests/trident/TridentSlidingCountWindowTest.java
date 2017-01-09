package com.jstorm.example.unittests.trident;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Consumer;
import storm.trident.testing.CountAsAggregator;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import storm.trident.windowing.InMemoryWindowsStoreFactory;
import storm.trident.windowing.WindowsStoreFactory;
import storm.trident.windowing.config.*;

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

/**
 * Created by binyang.dby on 2016/7/22.
 */
public class TridentSlidingCountWindowTest {

    private final static Logger LOG = LoggerFactory.getLogger(TridentSlidingCountWindowTest.class);
    private WindowConfig windowConfig = SlidingCountWindow.of(1000, 100);

    public final static int SPOUT_BATCH_SIZE = 3;
    public final static int SPOUT_LIMIT = 1000;

    @Test
    public void testTridentSlidingCountWindow()
    {
        WindowsStoreFactory windowsStoreFactory = new InMemoryWindowsStoreFactory();
        FixedLimitBatchSpout spout = new FixedLimitBatchSpout(SPOUT_LIMIT, new Fields("sentence"), SPOUT_BATCH_SIZE,
                    new Values("the cow jumped over the moon"),
                    new Values("the man went to the store and bought some candy"),
                    new Values("four score and seven years ago"), new Values("how many apples can you eat"),
                    new Values("to be or not to be the person"));

        TridentTopology tridentTopology = new TridentTopology();

        Stream stream = tridentTopology.newStream("spout1", spout).parallelismHint(16)
                    .each(new Fields("sentence"), new Split(), new Fields("word"))
                    .window(windowConfig, windowsStoreFactory, new Fields("word"), new CountAsAggregator(), new Fields("count"))
                    .peek(new ValidateConsumer());

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "TridentSlidingCountWindowTest");

        JStormUnitTestRunner.submitTopology(tridentTopology.build(), null, 120, null);
    }

    private static class ValidateConsumer implements Consumer
    {
        private long expectValueType = 0;

        @Override
        public void accept(TridentTuple input) {
            LOG.info("Received tuple: [{}]", input);
                expectValueType += 100;
                if(expectValueType > 1000)
                    expectValueType = 1000;

                long receive = input.getLong(0);
                assertEquals(expectValueType, receive);

        }
    }
}
