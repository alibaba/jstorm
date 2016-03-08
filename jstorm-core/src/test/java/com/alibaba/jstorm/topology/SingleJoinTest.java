/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.topology;

import junit.framework.Assert;

import static org.junit.Assert.assertNotSame;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.utils.JStormUtils;

public class SingleJoinTest {
    private static Logger LOG = LoggerFactory.getLogger(SingleJoinTest.class);
    
    public static AtomicInteger receiveCounter = new AtomicInteger(0);

    @Test
    public void test_single_join() {
	receiveCounter.set(0);
        try {
            FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
            FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("gender", genderSpout);
            builder.setSpout("age", ageSpout);
            builder.setBolt("join", new SingleJoinBolt(new Fields("gender", "age"))).fieldsGrouping("gender", new Fields("id"))
                    .fieldsGrouping("age", new Fields("id"));

            Config conf = new Config();
            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("join-example", conf, builder.createTopology());

            for (int i = 0; i < 10; i++) {
                String gender;
                if (i % 2 == 0) {
                    gender = "male";
                } else {
                    gender = "female";
                }
                genderSpout.feed(new Values(i, gender));
            }

            for (int i = 9; i >= 0; i--) {
                ageSpout.feed(new Values(i, i + 20));
            }

            JStormUtils.sleepMs(60 * 1000);
            
            assertNotSame(0, receiveCounter.get());
            
            cluster.shutdown();
        } catch (Exception e) {
            Assert.fail("Failed to run SingleJoinExample");
        }
    }
}
