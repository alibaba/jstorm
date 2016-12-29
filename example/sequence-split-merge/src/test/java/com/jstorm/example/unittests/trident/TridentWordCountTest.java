package com.jstorm.example.unittests.trident;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.jstorm.example.unittests.utils.JStormUnitTestDRPCValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import com.jstorm.example.unittests.utils.JStormUnitTestValidator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by binyang.dby on 2016/7/8.
 *
 * basically the unit test version of TridentWordCount. But this test runs only in local cluster mode.
 *
 * @Test pass at 2016/7/19
 */
public class TridentWordCountTest
{
    public final static int SPOUT_CONTENT_TYPES = 5;//there is 5 types of content can spout emit
    public final static int SPOUT_BATCH_SIZE = 3;   //at most 3 batches are emitted each time
    public final static int SPOUT_LIMIT = 1000;     //should be times of (Math.ceil)(SPOUT_CONTENT_TYPES/SPOUT_BATCH_SIZE)
                                                    // to make sure the validator is right

    @Test
    public void testTridentWordCount()
    {
        LocalDRPC localDRPC = new LocalDRPC();

        FixedLimitBatchSpout spout = new FixedLimitBatchSpout(SPOUT_LIMIT, new Fields("sentence"), SPOUT_BATCH_SIZE,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));

        TridentTopology tridentTopology = new TridentTopology();
        TridentState wordCount = tridentTopology.newStream("spout", spout).parallelismHint(1)
                        .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                        .parallelismHint(16);

        tridentTopology.newDRPCStream("words", localDRPC)
                        .each(new Fields("args"), new Split(), new Fields("keyword")).groupBy(new Fields("keyword"))
                        .stateQuery(wordCount, new Fields("keyword"), new MapGet(), new Fields("result"))
                        .each(new Fields("result"), new FilterNull())
                        .aggregate(new Fields("result"), new Sum(), new Fields("sum"));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "TridentWordCountTest");

        JStormUnitTestValidator validator = new JStormUnitTestDRPCValidator(localDRPC)
        {
            Logger LOG = LoggerFactory.getLogger(JStormUnitTestValidator.class);

            @Override
            public boolean validate(Map config) {
                String queryResult = executeLocalDRPC("words", "the");
                queryResult = queryResult.substring(2, queryResult.length() - 2);   //the result is like [[8080]], so remove the [[]]
                int oneLoopNeedEmits = (int)Math.ceil(SPOUT_CONTENT_TYPES/(float)SPOUT_BATCH_SIZE);        //how many times of emit can finish a loop
                                                                                    // of all the spout content
                int loopTime = SPOUT_LIMIT/oneLoopNeedEmits;                        //the loop time of the LimitFixBatchSpout content
                int receiveCountOfThe = Integer.valueOf(queryResult);
                LOG.info("Final receive total " + receiveCountOfThe + " \"the\" when expected " + (loopTime * 5));
                boolean isCountOfTheRight = (receiveCountOfThe == loopTime * 5);    //5 "the" are in one loop

                queryResult = executeLocalDRPC("words", "be store kujou");         // query the word count of these 3 words total
                queryResult = queryResult.substring(2, queryResult.length() - 2);
                int receiveCountOfBeAndStore = Integer.valueOf(queryResult);
                LOG.info("Final receive total " + receiveCountOfBeAndStore + " \"be\" and \"store\" and \"kujou\" " +
                        "when expected " + (loopTime * 3));
                boolean isCountOfBeAndStoreRight = (receiveCountOfBeAndStore == loopTime * 3);  //2 "be" 1 "store" 0 "kujou" are in one loop

                return isCountOfTheRight && isCountOfBeAndStoreRight;
            }
        };

        try {
            boolean result = JStormUnitTestRunner.submitTopology(tridentTopology.build(), config, 90, validator);
            assertTrue("Topology should pass the validator", result);
        }
        finally {
            localDRPC.shutdown();
        }
    }

    private static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
}
