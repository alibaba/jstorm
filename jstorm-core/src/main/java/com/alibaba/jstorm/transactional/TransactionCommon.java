package com.alibaba.jstorm.transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.task.TopologyContext;

public class TransactionCommon {
    public static final String BARRIER_STREAM_ID = "barrier_stream";
    public static final String SUCCESS_STREAM_ID = "success_stream";

    public static final String BARRIER_SNAPSHOT_FIELD = "barrier_snapshot";
    public static final String BATCH_GROUP_ID_FIELD = "batch_group_id";

    public static final long INIT_BATCH_ID = 0;

    public static final String ENABLE_TRANSACTION_CHECK_POINT = "enable.transaction.check.point";

    public static final String TRANSACTION_STATEFUL_BOLT = "transaction.stateful.bolt";

    public static final Object COMMIT_FAIL = new Object();

    public static Set<String> getDownstreamComponents(String componentId, StormTopology topology) {
        Set<String> components = new HashSet<String>();

        Map<String, Bolt> bolts = topology.get_bolts();
        for (Entry<String, Bolt> entry : bolts.entrySet()) {
            String downstreamComponentId = entry.getKey();
            Bolt bolt = entry.getValue();
            Set<GlobalStreamId> input = bolt.get_common().get_inputs().keySet();
            for (GlobalStreamId stream : input) {
                String upstreamComponentId = stream.get_componentId();
                if (upstreamComponentId.equals(componentId)) {
                    components.add(downstreamComponentId);
                    break;
                }
            }
        }
        
        return components;
    }

    public static Set<String> getAllDownstreamComponents(String componentId, StormTopology topology) {
        return getAllDownstreamComponents(componentId, topology, new HashSet<String>());
    }

    public static Set<String> getAllDownstreamComponents(String componentId, StormTopology topology, Set<String> traversedComponents) {
        Set<String> components = new HashSet<String>();
        traversedComponents.add(componentId);
        
        Map<String, Bolt> bolts = topology.get_bolts();
        for (Entry<String, Bolt> entry : bolts.entrySet()) {
            String downstreamComponentId = entry.getKey();
            Bolt bolt = entry.getValue();
            Set<GlobalStreamId> input = bolt.get_common().get_inputs().keySet();
            for (GlobalStreamId stream : input) {
                String upstreamComponentId = stream.get_componentId();
                if (upstreamComponentId.equals(componentId) && !traversedComponents.contains(downstreamComponentId)) {
                    components.add(downstreamComponentId);
                    components.addAll(getAllDownstreamComponents(downstreamComponentId, topology, traversedComponents));
                    break;
                }
            }
        }
        
        return components;
    }

    public static Set<Integer> getDownstreamTasks(String componentId, TopologyContext context) {
        Set<Integer> tasks = new HashSet<Integer>();
        StormTopology topology = context.getRawTopology();

        Map<String, Bolt> bolts = topology.get_bolts();
        for (Entry<String, Bolt> entry : bolts.entrySet()) {
            String downstreamComponentId = entry.getKey();
            Bolt bolt = entry.getValue();
            Set<GlobalStreamId> input = bolt.get_common().get_inputs().keySet();
            for (GlobalStreamId stream : input) {
                String upstreamComponentId = stream.get_componentId();
                if (upstreamComponentId.equals(componentId)) {
                    tasks.addAll(context.getComponentTasks(downstreamComponentId));
                    break;
                }
            }
        }
        
        return tasks;
    }

    public static Set<String> getUpstreamSpouts(String componentId, TopologyContext context) {
        Set<String> spoutIds = new HashSet<String>();
        StormTopology topology = context.getRawTopology();
        Map<String, SpoutSpec> spouts = topology.get_spouts();
        for (Entry<String, SpoutSpec> entry : spouts.entrySet()) {
            String spoutId = entry.getKey();
            Set<String> downstreamComponentIds = getAllDownstreamComponents(spoutId, context.getRawTopology());
            if (downstreamComponentIds.contains(componentId)) {
                spoutIds.add(spoutId);
            }
        }
        return spoutIds;
    }

    public static Set<String> getUpstreamComponents(String componentId, TopologyContext context) {
        Set<String> upstreamComponents = new HashSet<String>();
        ComponentCommon componentCommon = Utils.getComponentCommon(context.getRawTopology(), componentId);
        Set<GlobalStreamId> input = componentCommon.get_inputs().keySet();
        for (GlobalStreamId stream : input) {
            upstreamComponents.add(stream.get_componentId());
        }
        return upstreamComponents;
    }

    public static Set<Integer> getUpstreamTasks(String componentId, TopologyContext context) {
        Set<String> upstreamComponents = getUpstreamComponents(componentId, context);
        return new HashSet<Integer>(context.getComponentsTasks(upstreamComponents));
    }

    public static Set<String> getInputStreamIds(TopologyContext context) {
        Set<String> ret = new HashSet<String>();
        Set<GlobalStreamId> inputs = context.getThisSources().keySet();
        for (GlobalStreamId streamId : inputs) {
            ret.add(streamId.get_streamId());
        }
        return ret;
    }

    public static boolean isUserDefStream(String streamName) {
        return !streamName.equals(BARRIER_STREAM_ID);
    }

    public static long nextTransactionBatchId(long batchId) {
        return ++batchId;
    }

    public static int groupIndex(StormTopology topology, String spoutName) {
        Set<String> spoutComponents = topology.get_spouts().keySet();
        return groupIds(spoutComponents).get(spoutName);
    }

    public static Map<String, Integer> groupIds(Set<String> names) {
        Collections.sort(new ArrayList<String>(names));
        Map<String, Integer> ret = new HashMap<String, Integer>();
        int i = 1;
        for (String name : names) {
            ret.put(name, i);
            i++;
        }
        return ret;
    }

    public static Set<String> getStatefulBolts(StormTopology topology) {
        Set<String> statefulBolts = new HashSet<String>();
        
        Map<String, Bolt> bolts = topology.get_bolts();
        for (Entry<String, Bolt> entry : bolts.entrySet()) {
            Bolt bolt = entry.getValue();
            Map conf = JStormUtils.parseJson(bolt.get_common().get_json_conf());
            if (JStormUtils.parseBoolean(conf.get(TRANSACTION_STATEFUL_BOLT), false)) {
                statefulBolts.add(entry.getKey());
            }
        }
        
        return statefulBolts;
    }

    public static Set<String> getEndBolts(StormTopology topology, String sourceId) {
        Set<String> endBolts = new HashSet<String>();

        Map<String, Bolt> bolts = topology.get_bolts();
        for (Entry<String, Bolt> entry : bolts.entrySet()) {
            Bolt bolt = entry.getValue();
            Map<String, StreamInfo> outputStreams = bolt.get_common().get_streams();
            if (outputStreams.size() == 0) {
                endBolts.add(entry.getKey());
            }
        }
        
        return endBolts;
    }
}