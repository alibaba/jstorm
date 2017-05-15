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
package com.alipay.dw.jstorm.example.deploy;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class DeployTopology {
    private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
    private final static String TOPOLOGY_NUMS = "topology.nums";
    //number of levels of bolts per topolgy
    private final static String TOPOLOGY_BOLTS_NUMS = "topology.bolt.nums";
    //message size
    private final static String TOPOLOGY_MESSAGE_SIZES = "topology.messagesize";

    private static Map conf = new HashMap<>();

    public void realMain(String[] args) throws Exception {

        String _name = "MetricTest";
/*        if (args.length > 0){
            conf = Utils.loadConf(args[0]);
        }*/

        int _killTopologyTimeout = JStormUtils.parseInt(conf.get(ConfigExtension.TASK_CLEANUP_TIMEOUT_SEC), 180);
        conf.put(ConfigExtension.TASK_CLEANUP_TIMEOUT_SEC, _killTopologyTimeout);

        int _numWorkers = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS), 6);

        int _numTopologies = JStormUtils.parseInt(conf.get(TOPOLOGY_NUMS), 1);
        int _spoutParallel = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 2);
        int _boltParallel = JStormUtils.parseInt(conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 4);
        int _messageSize = JStormUtils.parseInt(conf.get(TOPOLOGY_MESSAGE_SIZES), 10);
        int _numAcker = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 2);
        int _boltNum = JStormUtils.parseInt(conf.get(TOPOLOGY_BOLTS_NUMS), 3);
        boolean _ackEnabled = false;
        if (_numAcker > 0)
            _ackEnabled = true;

        for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("messageSpout",
                    new DeploySpoult(_messageSize, _ackEnabled), _spoutParallel);
            builder.setBolt("messageBolt1", new DeployBolt(), _boltParallel)
                    .shuffleGrouping("messageSpout");
            for (int levelNum = 2; levelNum <= _boltNum; levelNum++) {
                builder.setBolt("messageBolt" + levelNum, new DeployBolt(), _boltParallel)
                        .shuffleGrouping("messageBolt" + (levelNum - 1));
            }

            conf.put(Config.TOPOLOGY_WORKERS, _numWorkers);
            conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, _numAcker);
            StormSubmitter.submitTopology(_name + "_" + topoNum, conf, builder.createTopology());
        }
    }

    public static void main(String[] args) throws Exception {
        new DeployTopology().realMain(args);
    }

}