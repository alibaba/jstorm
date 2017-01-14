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
package com.alipay.dw.jstorm.example.userdefined.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

import backtype.storm.Config;
import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClientWrapper;
import backtype.storm.utils.Utils;

/**
 * This is a basic example of a Storm topology.
 */
public class UserDefinedWorkerTopology {
    
    private static final String SPOUT_NAME = "spout";
    private static final String BOLT1_NAME = "bolt1";
    private static final String BOLT2_NAME = "bolt2";
    
    public static class RandomIntegerSpout extends BaseRichSpout {
        private static final Logger  LOG   = LoggerFactory.getLogger(RandomIntegerSpout.class);
        private SpoutOutputCollector collector;
        private Random               rand;
        private long                 msgId = 0;
        private String               ip;
        private Integer              port;
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value", "ip", "port"));
        }
        
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.rand = new Random();
            
            ip = NetWorkUtils.ip();
            port = context.getThisWorkerPort();
        }
        
        @Override
        public void nextTuple() {
            Utils.sleep(10);
            collector.emit(new Values(rand.nextInt(1000), ip, port), msgId);
        }
        
        @Override
        public void ack(Object msgId) {
            LOG.debug("Got ACK for msgId : " + msgId);
        }
        
        @Override
        public void fail(Object msgId) {
            LOG.debug("Got FAIL for msgId : " + msgId);
        }
    }
    
    public static class CheckBolt extends BaseRichBolt {
        OutputCollector _collector;
        String          ip;
        Integer         port;
        boolean         differentNode;
        private TopologyContext topologyContext;
        
        public CheckBolt(boolean differentNode) {
            this.differentNode = differentNode;
        }
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            ip = NetWorkUtils.ip();
            port = context.getThisWorkerPort();
            topologyContext = context;
        }
        
        
        public void fail(Tuple tuple, OutputCollector collector) {
            _collector.fail(tuple);
            JStormUtils.reportError(topologyContext, "Receive invalid tuple");
        }
        
        @Override
        public void execute(Tuple tuple) {
            String sourceIp = tuple.getString(1);
            Integer sourcePort = tuple.getInteger(2);
            if (differentNode) {
                if (ip.equals(sourceIp)) {
                    fail(tuple, _collector);
                    return;
                } else if (port.equals(sourcePort)) {
                    fail(tuple, _collector);
                    return;
                }
                _collector.emit(tuple, new Values(tuple.getValue(0), ip, port));
                _collector.ack(tuple);
                return;
            } else {
                if (ip.equals(sourceIp) == false) {
                    fail(tuple, _collector);
                    return;
                }
                _collector.emit(tuple, new Values(tuple.getValue(0), ip, port));
                _collector.ack(tuple);
            }
            
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value", "ip", "port"));
        }
        
    }
    
    /**
     * Kill all old topology is easy to do test
     */
    public static void prepare() {
        JStormHelper.cleanCluster();
    }
    
    public static void setUserDefinedWorker(Config conf) throws Exception {
        hosts = JStormHelper.getSupervisorHosts();
        
        if (hosts.size() < 3) {
            Assert.fail("Failed to run current test due to too less supervisors");
            
        }
        
        for (String host : hosts) {
            System.out.println(host);
        }

        List<WorkerAssignment> userDefinedWorks = new ArrayList<WorkerAssignment>();
        
        /**
         * Bind spout-1 and one bolt1-1 together
         */
        WorkerAssignment spout1Assignment = new WorkerAssignment();
        spout1Assignment.addComponent(SPOUT_NAME, 1);
        spout1Assignment.addComponent(BOLT1_NAME, 1);
        spout1Assignment.setHostName(hosts.get(0));
        userDefinedWorks.add(spout1Assignment);
        
        /**
         * Set the spout-2 in the second host
         */
        WorkerAssignment spout2Assignment = new WorkerAssignment();
        spout2Assignment.addComponent(SPOUT_NAME, 1);
        spout2Assignment.setHostName(hosts.get(1));
        userDefinedWorks.add(spout2Assignment);
        
        /**
         * Set the bolt1-2 in the second host
         */
        WorkerAssignment bolt2Assignment = new WorkerAssignment();
        bolt2Assignment.addComponent(BOLT1_NAME, 1);
        bolt2Assignment.setHostName(hosts.get(1));
        userDefinedWorks.add(bolt2Assignment);
        
        /**
         * Set the bolt1-3 and bolt2-1/bolt2-2/bolt2-3 in the third host
         */
        WorkerAssignment bolt3Assignment = new WorkerAssignment();
        bolt3Assignment.addComponent(BOLT1_NAME, 1);
        bolt3Assignment.addComponent(BOLT2_NAME, 3);
        bolt3Assignment.setHostName(hosts.get(2));
        userDefinedWorks.add(bolt3Assignment);
        
        ConfigExtension.setUserDefineAssignment(conf, userDefinedWorks);
    }
    
    static boolean      isLocal      = true;
    static int          workerNumber = 4;
    static List<String> hosts;
    static Config       conf         = JStormHelper.getConfig(null);
    
    public static void test() {
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(SPOUT_NAME, new RandomIntegerSpout(), 2);
        builder.setBolt(BOLT1_NAME, new CheckBolt(false), 3).localOrShuffleGrouping(SPOUT_NAME);
        builder.setBolt(BOLT2_NAME, new CheckBolt(true), 3).localOrShuffleGrouping(SPOUT_NAME);
        
        Config conf = new Config();
        conf.setDebug(true);
        
        if (isLocal == false) {
            prepare();
            
            conf.setNumWorkers(workerNumber);
            
            try {
                setUserDefinedWorker(conf);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                Assert.fail("Failed to set user defined worker");
            }
        }
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        try {
            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60, new Validator(), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static class Validator implements Callback {
        
        public void verifySpoutAssignment(Set<ResourceWorkerSlot> spoutResourceWorkerSlots) {
            Set<String> sourceIps = new HashSet<String>();
            sourceIps.add(hosts.get(0));
            sourceIps.add(hosts.get(1));
            
            if (spoutResourceWorkerSlots.size() != sourceIps.size()) {
                Assert.fail(
                        "Spout's assignment is wrong, spout workslots:" + spoutResourceWorkerSlots + ", defined: " + sourceIps);
            }
            
            for (ResourceWorkerSlot workerSlot : spoutResourceWorkerSlots) {
                if (sourceIps.contains(workerSlot.getHostname()) == false) {
                    Assert.fail("Spout's assignment is wrong, spout workslots:" + spoutResourceWorkerSlots + ", defined: "
                            + sourceIps);
                }
            }
            
        }
        
        public void verifyBolt1Assignment(Set<ResourceWorkerSlot> spoutResourceWorkerSlots, Set<ResourceWorkerSlot> bolt1ResourceWorkerSlots) {
            Set<String> sourceIps = new HashSet<String>();
            sourceIps.add(hosts.get(0));
            sourceIps.add(hosts.get(1));
            sourceIps.add(hosts.get(2));
            
            if (bolt1ResourceWorkerSlots.size() != sourceIps.size()) {
                Assert.fail(
                        "Bol1's assignment is wrong, spout workslots:" + spoutResourceWorkerSlots + ", defined: " + sourceIps);
            }
            
            for (ResourceWorkerSlot workerSlot : bolt1ResourceWorkerSlots) {
                if (sourceIps.contains(workerSlot.getHostname()) == false) {
                    Assert.fail("Bolt1's assignment is wrong, spout workslots:" + spoutResourceWorkerSlots + ", defined: "
                            + sourceIps);
                }
            }
            
            ResourceWorkerSlot bindResourceWorkerSlot = null;
            for (ResourceWorkerSlot workerSlot : spoutResourceWorkerSlots) {
                if (workerSlot.getHostname().equals(hosts.get(0))) {
                    bindResourceWorkerSlot = workerSlot;
                }
            }
            
            if (bolt1ResourceWorkerSlots.contains(bindResourceWorkerSlot) == false) {
                Assert.fail("Bolt1's assignment is wrong, bolt1-1 and spout1-1 should bind in " + hosts.get(0));
            }
        }
        
        public void verifyBolt2Assignment(Set<ResourceWorkerSlot> bolt1ResourceWorkerSlots, Set<ResourceWorkerSlot> bolt2ResourceWorkerSlots) {
            Set<String> sourceIps = new HashSet<String>();
            sourceIps.add(hosts.get(2));
            
            if (bolt2ResourceWorkerSlots.size() != sourceIps.size()) {
                Assert.fail(
                        "Bol2's assignment is wrong, spout workslots:" + bolt2ResourceWorkerSlots + ", defined: " + sourceIps);
            }
            
            for (ResourceWorkerSlot workerSlot : bolt2ResourceWorkerSlots) {
                if (sourceIps.contains(workerSlot.getHostname()) == false) {
                    Assert.fail("Bolt2's assignment is wrong, spout workslots:" + bolt2ResourceWorkerSlots + ", defined: "
                            + sourceIps);
                }
            }
            
            ResourceWorkerSlot bindResourceWorkerSlot = null;
            for (ResourceWorkerSlot workerSlot : bolt1ResourceWorkerSlots) {
                if (workerSlot.getHostname().equals(hosts.get(2))) {
                    bindResourceWorkerSlot = workerSlot;
                }
            }
            
            if (bolt2ResourceWorkerSlots.contains(bindResourceWorkerSlot) == false) {
                Assert.fail("Bolt2's assignment is wrong, bolt1-2 and bolt2-1/bolt2-2/bolt2-3 should bind in "
                        + hosts.get(0));
            }
        }
        
        public Set<ResourceWorkerSlot> getComponentWorkers(ComponentSummary component, Set<ResourceWorkerSlot> workerSet ) {
            Set<ResourceWorkerSlot> workerSlots = new HashSet<>();
            
            for (int taskId : component.get_taskIds()) {
                for (ResourceWorkerSlot worker : workerSet) {
                    if (worker.getTasks().contains(taskId)) {
                        workerSlots.add(worker);
                    }
                }
            }
            
            return workerSlots;
        }
        
        public void verifyAssignment(String topologyName) {

            
            Set<ResourceWorkerSlot> spoutResourceWorkerSlots = new HashSet<>();
            Set<ResourceWorkerSlot> bolt1ResourceWorkerSlots = new HashSet<>();
            Set<ResourceWorkerSlot> bolt2ResourceWorkerSlots = new HashSet<>();
            
            NimbusClientWrapper client = new NimbusClientWrapper();
            try {
                client.init(conf);
                
                String topologyId = client.getClient().getTopologyId(topologyName);
                
                TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topologyId);
                
                Assignment assignment = JStormHelper.getAssignment(topologyId, conf);
                Set<ResourceWorkerSlot> workerSet = assignment.getWorkers();
                
                List<ComponentSummary>  componentList = topologyInfo.get_components();
                for (ComponentSummary component : componentList) {
                    if (SPOUT_NAME.equals(component.get_name())) {
                        spoutResourceWorkerSlots = getComponentWorkers(component, workerSet);
                    }else if (BOLT1_NAME.equals(component.get_name())) {
                        bolt1ResourceWorkerSlots = getComponentWorkers(component, workerSet);
                    }else if (BOLT2_NAME.equals(component.get_name())) {
                        bolt2ResourceWorkerSlots = getComponentWorkers(component, workerSet);
                    }
                }
                
            }catch(Exception e) {
                Assert.fail("Fail to get workerSlots");
            }finally {
                client.cleanup();
            }
            
            
            verifySpoutAssignment(spoutResourceWorkerSlots);
            verifyBolt1Assignment(spoutResourceWorkerSlots, bolt1ResourceWorkerSlots);
            verifyBolt2Assignment(bolt1ResourceWorkerSlots, bolt2ResourceWorkerSlots);
            
        }
        
        
        
        @Override
        public <T> Object execute(T... args) {
            // TODO Auto-generated method stub
            String topologyName = (String) args[0];
            if (isLocal == true) {
                Callback callback = new JStormHelper.CheckAckedFail(conf);
                return callback.execute(args);
                
            }
            
            JStormUtils.sleepMs(60 * 1000);
            
            verifyAssignment(topologyName);
            
            Callback callback = new JStormHelper.CheckAckedFail(conf);
            return callback.execute(args);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        isLocal = false;
        conf = JStormHelper.getConfig(args);
        if (args.length != 0) {
            workerNumber = 5;
        }
        test();
    }
}
