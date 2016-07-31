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
package com.alipay.dw.jstorm.example.sequence;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;
import com.alipay.dw.jstorm.example.sequence.bolt.MergeRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.PairCount;
import com.alipay.dw.jstorm.example.sequence.bolt.SplitRecord;
import com.alipay.dw.jstorm.example.sequence.bolt.TotalCount;
import com.alipay.dw.jstorm.example.sequence.kryo.PairSerializer;
import com.alipay.dw.jstorm.example.sequence.kryo.TradeCustomerSerializer;
import com.alipay.dw.jstorm.example.sequence.spout.SequenceSpout;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceTopology {
    private static Logger LOG = LoggerFactory.getLogger(SequenceTopology.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";

    public static void SetBuilder(TopologyBuilder builder, Map conf) {

        int spout_Parallelism_hint = JStormUtils.parseInt(
                conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int bolt_Parallelism_hint = JStormUtils.parseInt(
                conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);

        builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
                new SequenceSpout(), spout_Parallelism_hint);

        boolean isEnableSplit = JStormUtils.parseBoolean(conf.get("enable.split"), false);

        if (!isEnableSplit) {
            BoltDeclarer boltDeclarer = builder.setBolt(
                    SequenceTopologyDef.TOTAL_BOLT_NAME, new TotalCount(),
                    bolt_Parallelism_hint);

            // localFirstGrouping is only for jstorm
            // boltDeclarer.localFirstGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
            boltDeclarer
                    .shuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME)
                    .allGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME, SequenceTopologyDef.CONTROL_STREAM_ID)
                    .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        } else {

            builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME,
                    new SplitRecord(), bolt_Parallelism_hint)
                    .localOrShuffleGrouping(
                            SequenceTopologyDef.SEQUENCE_SPOUT_NAME);

            builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME,
                    new PairCount(), bolt_Parallelism_hint).shuffleGrouping(
                    SequenceTopologyDef.SPLIT_BOLT_NAME,
                    SequenceTopologyDef.TRADE_STREAM_ID);
            builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME,
                    new PairCount(), bolt_Parallelism_hint).shuffleGrouping(
                    SequenceTopologyDef.SPLIT_BOLT_NAME,
                    SequenceTopologyDef.CUSTOMER_STREAM_ID);

            builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME,
                    new MergeRecord(), bolt_Parallelism_hint)
                    .fieldsGrouping(SequenceTopologyDef.TRADE_BOLT_NAME,
                            new Fields("ID"))
                    .fieldsGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME,
                            new Fields("ID"));

            builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME,
                    new TotalCount(), bolt_Parallelism_hint).noneGrouping(
                    SequenceTopologyDef.MERGE_BOLT_NAME);
        }

        boolean kryoEnable = JStormUtils.parseBoolean(conf.get("kryo.enable"), false);
        if (kryoEnable) {
            System.out.println("Use Kryo ");
            boolean useJavaSer = JStormUtils.parseBoolean(
                    conf.get("fall.back.on.java.serialization"), true);

            Config.setFallBackOnJavaSerialization(conf, useJavaSer);

            Config.registerSerialization(conf, TradeCustomer.class, TradeCustomerSerializer.class);
            Config.registerSerialization(conf, Pair.class, PairSerializer.class);
        }

        // conf.put(Config.TOPOLOGY_DEBUG, false);
        // conf.put(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE, false);
        // conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        int ackerNum = JStormUtils.parseInt(
                conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
        Config.setNumAckers(conf, ackerNum);
        // conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
        // conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        // conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS), 20);
        conf.put(Config.TOPOLOGY_WORKERS, workerNum);

    }

    public static void SetLocalTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, 1);
        SetBuilder(builder, conf);

        LOG.debug("test");
        LOG.info("Submit log");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("SplitMerge", conf, builder.createTopology());

        Thread.sleep(60000);
        cluster.killTopology("SplitMerge");
        cluster.shutdown();
    }

    public static void SetRemoteTopology() throws AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {
        String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
        if (streamName == null) {
            streamName = "SequenceTest";
        }

        TopologyBuilder builder = new TopologyBuilder();
        SetBuilder(builder, conf);
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");

        StormSubmitter.submitTopology(streamName, conf, builder.createTopology());
    }

    public static void SetDPRCTopology() throws AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {
        // LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
        // "exclamation");
        //
        // builder.addBolt(new TotalCount(), 3);
        //
        // Config conf = new Config();
        //
        // conf.setNumWorkers(3);
        // StormSubmitter.submitTopology("rpc", conf,
        // builder.createRemoteTopology());
        System.out.println("Please refer to com.alipay.dw.jstorm.example.drpc.ReachTopology");
    }

    private static Map conf = new HashMap<Object, Object>();


    public static void LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            conf = LoadConf.LoadYaml(arg);
        } else {
            conf = LoadConf.LoadProperty(arg);
        }
    }

    public static boolean local_mode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if (mode.equals("local")) {
                return true;
            }
        }

        return false;

    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Please input configuration file");
            System.exit(-1);
        }

        LoadConf(args[0]);
        if (local_mode(conf)) {
            SetLocalTopology();
        } else {
            SetRemoteTopology();
        }
    }

}
