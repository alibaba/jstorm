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

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.JStormUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TransactionCommon {
    public static final String BARRIER_STREAM_ID = "barrier_stream";

    public static final String BARRIER_SNAPSHOT_FIELD = "barrier_snapshot";
    public static final String BATCH_GROUP_ID_FIELD = "batch_group_id";

    public static final long INIT_BATCH_ID = 0;

    public static final String ENABLE_TRANSACTION_CHECK_POINT = "enable.transaction.check.point";

    public static final String TRANSACTION_STATEFUL_BOLT = "transaction.stateful.bolt";

    public static final Object COMMIT_FAIL = new Object();

    public static Set<String> getDownstreamComponents(String componentId, StormTopology topology) {
        Set<String> components = new HashSet<>();

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

    public static Set<String> getAllDownstreamComponents(String componentId, StormTopology topology,
                                                         Set<String> traversedComponents) {
        Set<String> components = new HashSet<>();
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
        Set<Integer> tasks = new HashSet<>();
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
        Set<String> spoutIds = new HashSet<>();
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
        Set<String> upstreamComponents = new HashSet<>();
        ComponentCommon componentCommon = Utils.getComponentCommon(context.getRawTopology(), componentId);
        Set<GlobalStreamId> input = componentCommon.get_inputs().keySet();
        for (GlobalStreamId stream : input) {
            upstreamComponents.add(stream.get_componentId());
        }
        return upstreamComponents;
    }

    public static Set<Integer> getUpstreamTasks(String componentId, TopologyContext context) {
        Set<String> upstreamComponents = getUpstreamComponents(componentId, context);
        return new HashSet<>(context.getComponentsTasks(upstreamComponents));
    }

    public static Set<String> getInputStreamIds(TopologyContext context) {
        Set<String> ret = new HashSet<>();
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
        Collections.sort(new ArrayList<>(names));
        Map<String, Integer> ret = new HashMap<>();
        int i = 1;
        for (String name : names) {
            ret.put(name, i);
            i++;
        }
        return ret;
    }

    public static Set<String> getStatefulBolts(StormTopology topology) {
        Set<String> statefulBolts = new HashSet<>();

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

    public static Set<String> getEndBolts(StormTopology topology) {
        Set<String> endBolts = new HashSet<>();

        Map<String, Bolt> bolts = topology.get_bolts();
        for (Entry<String, Bolt> entry : bolts.entrySet()) {
            String componentId = entry.getKey();
            Bolt bolt = entry.getValue();
            Map<String, StreamInfo> outputStreams = bolt.get_common().get_streams();
            if (outputStreams.size() == 0) {
                endBolts.add(entry.getKey());  
            } else {
                // Try to check if there is any "real" downstream for the output stream
                boolean found = false;
                for (String streamId : outputStreams.keySet()) {
                    if (hasDownstreamComponent(topology, componentId, streamId)) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    endBolts.add(entry.getKey()); 
            }
        }

        return endBolts;
    }

    private static boolean hasDownstreamComponent(StormTopology topology, String outComponentId, String outStreamId) {
        boolean ret = false;
        for (String componentId : ThriftTopologyUtils.getComponentIds(topology)) {
            ComponentCommon componentCommon = Utils.getComponentCommon(topology, componentId);
            Set<GlobalStreamId> inputs = componentCommon.get_inputs().keySet();
            for (GlobalStreamId input : inputs) {
                if (input.get_componentId().equals(outComponentId) && input.get_streamId().equals(outStreamId))
                    return true;
            }
        }
        return ret;
    }
}