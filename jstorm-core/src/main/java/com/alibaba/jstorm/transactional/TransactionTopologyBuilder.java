package com.alibaba.jstorm.transactional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.transactional.bolt.ITransactionBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.ITransactionStatefulBoltExecutor;
import com.alibaba.jstorm.transactional.bolt.TransactionBolt;
import com.alibaba.jstorm.transactional.bolt.TransactionStatefulBolt;
import com.alibaba.jstorm.transactional.spout.BasicTransactionSpout;
import com.alibaba.jstorm.transactional.spout.ITransactionSpoutExecutor;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class TransactionTopologyBuilder extends TopologyBuilder {
    private Map<String, Set<String>> upToDownstreamComponentsMap = new HashMap<String, Set<String>>();

    @Override
    public StormTopology createTopology() {
        System.setProperty(ConfigExtension.TASK_BATCH_TUPLE, "true");
        System.setProperty(Config.TOPOLOGY_ACKER_EXECUTORS, "0");
        return super.createTopology();
    }

    public BoltDeclarer setBolt(String id, ITransactionBoltExecutor bolt) {
        return setBolt(id, bolt, null);
    }

    public BoltDeclarer setBolt(String id, ITransactionBoltExecutor bolt, Number parallelism_hint) {
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
        initCommon(id, boltExecutor, parallelism_hint);
        _bolts.put(id, boltExecutor);
        BoltDeclarer ret = new TransactionBoltDeclarer(id);
        ret.addConfiguration(TransactionCommon.TRANSACTION_STATEFUL_BOLT, isStatefulBolt);
        return ret;
    }

    public SpoutDeclarer setSpout(String id, ITransactionSpoutExecutor spout) {
        return setSpout(id, spout, null);
    }

    public SpoutDeclarer setSpout(String id, ITransactionSpoutExecutor spout, Number parallelism_hint) {
        upToDownstreamComponentsMap.put(id, new HashSet<String>());
        IRichSpout spoutExecutor = new BasicTransactionSpout((ITransactionSpoutExecutor) spout);
        return super.setSpout(id, spoutExecutor, parallelism_hint);
    }

    public class TransactionBoltDeclarer extends BoltGetter {
        public TransactionBoltDeclarer(String boltId) {
            super(boltId);
        }

        protected BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            // Add barrier snapshot stream and success stream for transaction topology
            Set<String> downstreamBolts = upToDownstreamComponentsMap.get(componentId);
            if (downstreamBolts != null && downstreamBolts.contains(_boltId) == false) {
                downstreamBolts.add(_boltId);
                _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, TransactionCommon.BARRIER_STREAM_ID), 
                        Grouping.all(new NullStruct()));
            }
            _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }
    }
}