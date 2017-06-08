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
package com.alibaba.jstorm.transactional;

import backtype.storm.Config;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.transactional.bolt.AckTransactionBolt;
import com.alibaba.jstorm.transactional.bolt.ITransactionBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.ITransactionStatefulBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.KvStatefulBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.TransactionBolt;
import com.alibaba.jstorm.transactional.bolt.TransactionStatefulBolt;
import com.alibaba.jstorm.transactional.spout.AckTransactionSpout;
import com.alibaba.jstorm.transactional.spout.BasicTransactionSpout;
import com.alibaba.jstorm.transactional.spout.IBasicTransactionSpoutExecutor;
import com.alibaba.jstorm.transactional.spout.ITransactionSpoutExecutor;
import com.alibaba.jstorm.transactional.spout.ScheduleTransactionSpout;
import com.alibaba.jstorm.transactional.state.task.KeyRangeStateTaskInit;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.window.BaseWindowedBolt;
import com.alibaba.jstorm.window.TransactionalWindowedBoltExecutor;
import com.alibaba.jstorm.window.WindowAssigner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionTopologyBuilder extends TopologyBuilder {
    public static Logger LOG = LoggerFactory.getLogger(TransactionTopologyBuilder.class);

    private Map<String, Set<String>> upToDownstreamComponentsMap = new HashMap<>();

    @Override
    public StormTopology createTopology() {
        TopologyBuilder.putStormConf(ConfigExtension.TASK_BATCH_TUPLE, "true");
        TopologyBuilder.putStormConf(Config.TOPOLOGY_ACKER_EXECUTORS, "0");
        TopologyBuilder.putStormConf(ConfigExtension.TRANSACTION_TOPOLOGY, true);
        return super.createTopology();
    }

    /**
     * Set configuration of hdfs env
     */
    public void enableHdfs() {
        TopologyBuilder.putStormConf("worker.external", "hdfs");
        TopologyBuilder.putStormConf(ConfigExtension.TOPOLOGY_TRANSACTION_STATE_OPERATOR_CLASS,
                "com.alibaba.jstorm.hdfs.transaction.HdfsTransactionTopoStateImpl");
    }

    /********************** build spout declarer ***********************/
    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) throws IllegalArgumentException {
        return setSpout(id, spout, parallelismHint, true);
    }

    public SpoutDeclarer setSpout(String id, ITransactionSpoutExecutor spout) {
        return setSpout(id, spout, null);
    }

    public SpoutDeclarer setSpout(String id, ITransactionSpoutExecutor spout, Number parallelismHint) {
        return setSpout(id, spout, parallelismHint, true);
    }

    public SpoutDeclarer setSpout(String id, ITransactionSpoutExecutor spout, Number parallelismHint, boolean isSchedule) {
        return setSpout(id, spout, parallelismHint, isSchedule);
    }

    private SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint, boolean isSchedule) {
        upToDownstreamComponentsMap.put(id, new HashSet<String>());
        IRichSpout spoutExecutor;
        if (spout instanceof IBasicTransactionSpoutExecutor) {
            spoutExecutor = new BasicTransactionSpout((IBasicTransactionSpoutExecutor) spout);
        } else if (!isSchedule) {
            spoutExecutor = new BasicTransactionSpout((ITransactionSpoutExecutor) spout);
        } else {
            spoutExecutor = new ScheduleTransactionSpout((ITransactionSpoutExecutor) spout);
        }
        SpoutDeclarer ret = super.setSpout(id, spoutExecutor, parallelismHint);
        return ret;
    }

    /**
     * Build spout to provide the compatibility with Storm's ack mechanism
     *
     * @param id spout Id
     * @param spout
     * @return
     */
    public SpoutDeclarer setSpoutWithAck(String id, IRichSpout spout, Number parallelismHint) {
        return setSpout(id, new AckTransactionSpout(spout), parallelismHint);
    }

    /********************** build bolt declarer ***********************/
    public BoltDeclarer setBolt(String id, ITransactionBoltExecutor bolt) {
        return setBolt(id, bolt, null);
    }

    public BoltDeclarer setBolt(String id, ITransactionBoltExecutor bolt, Number parallelismHint) {
        return setBolt(id, bolt, parallelismHint);
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) throws IllegalArgumentException{
        upToDownstreamComponentsMap.put(id, new HashSet<String>());
        validateUnusedId(id);
        IRichBolt boltExecutor;
        boolean isStatefulBolt = false;
        if (bolt instanceof ITransactionStatefulBoltExecutor) {
            isStatefulBolt = true;
            boltExecutor = new TransactionStatefulBolt((ITransactionStatefulBoltExecutor) bolt);
        } else {
            boltExecutor = new TransactionBolt((ITransactionBoltExecutor) bolt);
        }
        initCommon(id, boltExecutor, parallelismHint);
        _bolts.put(id, boltExecutor);
        BoltDeclarer ret = new TransactionBoltDeclarer(id);
        ret.addConfiguration(TransactionCommon.TRANSACTION_STATEFUL_BOLT, isStatefulBolt);

        // If using KvState bolt, the corresponding init operater would be registered here.
        if (bolt instanceof KvStatefulBoltExecutor) {
            ConfigExtension.registerTransactionTaskStateInitOp(TopologyBuilder.getStormConf(), id, KeyRangeStateTaskInit.class);
        }

        return ret;
    }

    /**
     * Define a new bolt in this topology. This defines a windowed bolt, intended
     * for windowing operations.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the windowed bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, BaseWindowedBolt<Tuple> bolt, Number parallelism_hint)
            throws IllegalArgumentException {
        boolean isEventTime = WindowAssigner.isEventTime(bolt.getWindowAssigner());
        if (isEventTime && bolt.getTimestampExtractor() == null) {
            throw new IllegalArgumentException("timestamp extractor must be defined in event time!");
        }
        return setBolt(id, new TransactionalWindowedBoltExecutor(bolt), parallelism_hint);
    }

    /**
     * Build bolt to provide the compatibility with Storm's ack mechanism
     *
     * @param id bolt Id
     * @param bolt
     * @return
     */
    public BoltDeclarer setBoltWithAck(String id, IRichBolt bolt, Number parallelismHint) {
        return setBolt(id, new AckTransactionBolt(bolt), parallelismHint);
    }

    /*************************** TransactionBoltDeclarer **************************/
    public class TransactionBoltDeclarer extends BoltGetter {
        public TransactionBoltDeclarer(String boltId) {
            super(boltId);
        }

        protected BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            // Check if bolt is KvStateBolt, if so, enable the key range hash in upstream component
            TransactionBolt bolt = (TransactionBolt) _bolts.get(_boltId);
            if (bolt.getBoltExecutor() instanceof KvStatefulBoltExecutor) {
                ComponentCommon common = _commons.get(componentId);
                Map<String, Object> conf = new HashMap<>();
                conf.put(ConfigExtension.ENABLE_KEY_RANGE_FIELD_GROUP, true);
                String currConf = common.get_json_conf();
                common.set_json_conf(JStormUtils.mergeIntoJson(JStormUtils.parseJson(currConf), conf));
            }

            // Add barrier snapshot stream for transaction topology
            Set<String> downstreamBolts = upToDownstreamComponentsMap.get(componentId);
            if (downstreamBolts != null && !downstreamBolts.contains(_boltId)) {
                downstreamBolts.add(_boltId);
                _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, TransactionCommon.BARRIER_STREAM_ID), Grouping.all(new NullStruct()));
            }
            _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            //return this;
            return super.grouping(componentId, streamId, grouping);
        }
    }
}