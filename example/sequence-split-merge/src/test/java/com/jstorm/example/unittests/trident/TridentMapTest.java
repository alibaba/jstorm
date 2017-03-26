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
import storm.trident.operation.BaseFilter;
import storm.trident.operation.Consumer;
import storm.trident.operation.FlatMapFunction;
import storm.trident.operation.MapFunction;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Created by binyang.dby on 2016/7/8.
 *
 * basically the unit test version of the TridentMapExample.
 *
 * @Test pass at 2016/7/19
 */
public class TridentMapTest
{
    private static Logger LOG = LoggerFactory.getLogger(TridentMapTest.class);

    public final static int SPOUT_CONTENT_TYPES = 5;//there is 5 types of content can spout emit
    public final static int SPOUT_BATCH_SIZE = 3;   //at most 3 batches are emitted each time
    public final static int SPOUT_LIMIT = 1000;     //should be times of (Math.ceil)(SPOUT_CONTENT_TYPES/SPOUT_BATCH_SIZE)
                                                    // to make sure the validator is right

    @Test
    public void testTridentMap()
    {
        LocalDRPC localDRPC = new LocalDRPC();

        FixedLimitBatchSpout spout = new FixedLimitBatchSpout(SPOUT_LIMIT, new Fields("word"), SPOUT_BATCH_SIZE,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));

        TridentTopology tridentTopology = new TridentTopology();
        TridentState wordCount = tridentTopology.newStream("spout", spout).parallelismHint(1)
                        .flatMap(new SplitFlatMapFunction()).map(new UpperMapFunction())
                        .filter(new Fields("word"), new WordFilter("THE"))
                        .peek(new LogConsumer()).groupBy(new Fields("word"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                        .parallelismHint(16);

        tridentTopology.newDRPCStream("words", localDRPC)
                        .flatMap(new SplitFlatMapFunction()).groupBy(new Fields("args"))
                        .stateQuery(wordCount, new Fields("args"), new MapGet(), new Fields("result"))
                        .filter(new Fields("result"), new FilterNull())
                        .aggregate(new Fields("result"), new Sum(), new Fields("sum"));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "TridentMapTest");

        JStormUnitTestValidator validator = new JStormUnitTestDRPCValidator(localDRPC)
        {
            Logger LOG = LoggerFactory.getLogger(JStormUnitTestValidator.class);

            @Override
            public boolean validate(Map config) {
                String queryResult = executeLocalDRPC("words", "THE");
                queryResult = queryResult.substring(2, queryResult.length() - 2);   //the result is like [[8080]], so remove the [[]]
                int oneLoopNeedEmits = (int)Math.ceil(SPOUT_CONTENT_TYPES/(float)SPOUT_BATCH_SIZE);        //how many times of emit can finish a loop
                // of all the spout content
                int loopTime = SPOUT_LIMIT/oneLoopNeedEmits;                        //the loop time of the LimitFixBatchSpout content
                int receiveCountOfUpperCase = Integer.valueOf(queryResult);
                LOG.info("Final receive total " + receiveCountOfUpperCase + " \"THE\" when expected " + (loopTime * 5));
                boolean isCountOfUpperCaseRight = (receiveCountOfUpperCase == loopTime * 5);    //5 "the" are in one loop

                queryResult = executeLocalDRPC("words", "the tHe thE");            //query the word count of these 3 words total
                queryResult = queryResult.substring(2, queryResult.length() - 2);
                int receiveCountOfLowerCase = Integer.valueOf(queryResult);
                LOG.info("Final receive total " + receiveCountOfLowerCase + " \"the\" and \"tHe\" and \"thE\" " +
                        "when expected " + 0);
                boolean isCountOfLowerCaseRight = (receiveCountOfLowerCase == 0);  //no the tHe thE should pass the UpperFilter

                return isCountOfUpperCaseRight && isCountOfLowerCaseRight;
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

    private static class SplitFlatMapFunction implements FlatMapFunction
    {
        @Override
        public Iterable<Values> execute(TridentTuple input){
            String[] split = input.getString(0).split(" ");
            List<Values> tuple = new ArrayList<Values>();
            for(String value : split)
                tuple.add(new Values(value));
            return tuple;
        }
    }

    private static class UpperMapFunction implements MapFunction
    {
        @Override
        public Values execute(TridentTuple input){
            return new Values(input.getString(0).toUpperCase());
        }
    }

    private static class WordFilter extends BaseFilter
    {
        private String word;

        public WordFilter(String word){
            this.word = word;
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getString(0).equals(word);
        }
    }

    private static class LogConsumer implements Consumer
    {
        @Override
        public void accept(TridentTuple input) {
            LOG.info(input.getString(0) + "flow pass the LogConsumer.");
        }
    }
}
