package com.jstorm.example.unittests.trident;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import com.jstorm.example.unittests.utils.JStormUnitTestDRPCValidator;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.apache.storm.starter.trident.TridentReach;
import org.junit.Test;
import static org.junit.Assert.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/22.
 */
public class TridentReachTest {
    public static Map<String, List<String>> TWEETERS = new HashMap<String, List<String>>() {
        {
            put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
            put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
            put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
        }
    };

    public static Map<String, List<String>> FOLLOWERS = new HashMap<String, List<String>>() {
        {
            put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
            put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
            put("tim", Arrays.asList("alex"));
            put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
            put("adam", Arrays.asList("david", "carissa"));
            put("mike", Arrays.asList("john", "bob"));
            put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
        }
    };

    @Test
    public void testTridentReach()
    {
        TridentTopology tridentTopology = new TridentTopology();
        TridentState urlToTweeters = tridentTopology.newStaticState(new TridentReach.StaticSingleKeyMapState.Factory(TWEETERS));
        TridentState tweetersToFollowers = tridentTopology.newStaticState(new TridentReach.StaticSingleKeyMapState.Factory(FOLLOWERS));

        LocalDRPC localDRPC = new LocalDRPC();

        tridentTopology.newDRPCStream("reach", localDRPC)
                .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
                .each(new Fields("tweeters"), new TridentReach.ExpandList(), new Fields("tweeter")).shuffle()
                .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
                .each(new Fields("followers"), new TridentReach.ExpandList(), new Fields("follower")).groupBy(new Fields("follower"))
                .aggregate(new TridentReach.One(), new Fields("one")).aggregate(new Fields("one"), new Sum(), new Fields("reach"));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "TridentReachTest");

        JStormUnitTestDRPCValidator validator = new JStormUnitTestDRPCValidator(localDRPC) {
            @Override
            public boolean validate(Map config) {
                String query = executeLocalDRPC("reach", "aaa");
                assertEquals("[[0]]", query);

                query = executeLocalDRPC("reach", "foo.com/blog/1");
                assertEquals("[[16]]", query);

                query = executeLocalDRPC("reach", "engineering.twitter.com/blog/5");
                assertEquals("[[14]]", query);
                return true;
            }
        };

        try {
            JStormUnitTestRunner.submitTopology(tridentTopology.build(), config, 120, validator);
        }
        finally {
            localDRPC.shutdown();
        }
    }
}
