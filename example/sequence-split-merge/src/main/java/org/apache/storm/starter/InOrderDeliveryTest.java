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
package org.apache.storm.starter;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class InOrderDeliveryTest {
    public static class InOrderSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        int                  _base = 0;
        int                  _i    = 0;
        
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _base = context.getThisTaskIndex();
        }
        
        @Override
        public void nextTuple() {
            Values v = new Values(_base, _i);
            _collector.emit(v, "ACK");
            _i++;
        }
        
        @Override
        public void ack(Object id) {
            // Ignored
        }
        
        @Override
        public void fail(Object id) {
            // Ignored
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("c1", "c2"));
        }
    }
    
    public static class Check extends BaseBasicBolt {
        Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
        
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Integer c1 = tuple.getInteger(0);
            Integer c2 = tuple.getInteger(1);
            Integer exp = expected.get(c1);
            if (exp == null)
                exp = 0;
            if (c2.intValue() != exp.intValue()) {
                System.out.println(c1 + " " + c2 + " != " + exp);
                throw new FailedException(c1 + " " + c2 + " != " + exp);
            }
            exp = c2 + 1;
            expected.put(c1, exp);
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // Empty
        }
    }
    
    static boolean isLocal = true;
    static Config  conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        TopologyBuilder builder = new TopologyBuilder();
        
        int spoutNum = JStormUtils.parseInt(conf.get("spout.num"), 8);
        int countNum = JStormUtils.parseInt(conf.get("count.num"), 8);

        builder.setSpout("spout", new InOrderSpout(), spoutNum);
        builder.setBolt("count", new Check(), countNum).fieldsGrouping("spout", new Fields("c1"));
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        conf = JStormHelper.getConfig(args);
        isLocal = false;
        test();
    }
    
}
