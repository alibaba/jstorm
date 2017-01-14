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
package com.alibaba.jstorm.task.comm;

import java.util.*;
import java.util.Map.Entry;

import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.daemon.worker.JStormDebugger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;

import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.execute.MsgInfo;
import com.alibaba.jstorm.task.group.GrouperType;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * tuple sending object, which get which task should tuple be send to, and update statics
 * 
 * @author yannian/Longda
 * 
 */
public class TaskSendTargets {
    private static Logger LOG = LoggerFactory.getLogger(TaskSendTargets.class);

    private Map<Object, Object> stormConf;
    // it is system TopologyContext
    private TopologyContext topologyContext;

    // <Stream_id,<component, Grouping>>
    private volatile Map<String, Map<String, MkGrouper>> streamComponentgrouper;
    // SpoutTaskStatsRolling or BoltTaskStatsRolling
    private TaskBaseMetric taskStats;

    private String componentId;
    private int taskId;
    private String debugIdStr;

    public TaskSendTargets(Map<Object, Object> _storm_conf, String _component, Map<String, Map<String, MkGrouper>> _stream_component_grouper,
            TopologyContext _topology_context, TaskBaseMetric _task_stats) {
        this.stormConf = _storm_conf;
        this.componentId = _component;
        this.streamComponentgrouper = _stream_component_grouper;
        this.topologyContext = _topology_context;
        this.taskStats = _task_stats;


        taskId = topologyContext.getThisTaskId();
        debugIdStr = " emit from " + componentId + ":" + taskId + " ";
    }

    // direct send tuple to special task
    public List<Integer> get(Integer out_task_id, String stream, List<Object> tuple, Collection<Tuple> anchors, Object root_id) {

        // in order to improve acker's speed, skip checking
        // String target_component =
        // topologyContext.getComponentId(out_task_id);
        // Map<String, MkGrouper> component_prouping = streamComponentgrouper
        // .get(stream);
        // MkGrouper grouping = component_prouping.get(target_component);
        // if (grouping != null &&
        // !GrouperType.direct.equals(grouping.gettype())) {
        // throw new IllegalArgumentException(
        // "Cannot emitDirect to a task expecting a regular grouping");
        // }

        if (isDebug(anchors, root_id)) {
            LOG.info(debugIdStr + stream + " to " + out_task_id + ":" + tuple.toString());
        }

        taskStats.send_tuple(stream, 1);

        List<Integer> out_tasks = new ArrayList<Integer>();
        out_tasks.add(out_task_id);
        return out_tasks;
    }

    // send tuple according to grouping
    public List<Integer> get(String stream, List<Object> tuple, Collection<Tuple> anchors, Object root_id) {
        List<Integer> out_tasks = new ArrayList<Integer>();

        // get grouper, then get which task should tuple be sent to.
        Map<String, MkGrouper> componentCrouping = streamComponentgrouper.get(stream);
        if (componentCrouping == null) {
            // if the target component's parallelism is 0, don't need send to
            // them
            LOG.debug("Failed to get Grouper of " + stream + " when " + debugIdStr);
            return out_tasks;
        }

        for (Entry<String, MkGrouper> ee : componentCrouping.entrySet()) {
            String targetComponent = ee.getKey();
            MkGrouper g = ee.getValue();

            if (GrouperType.direct.equals(g.gettype())) {
                throw new IllegalArgumentException("Cannot do regular emit to direct stream");
            }

            out_tasks.addAll(g.grouper(tuple));

        }

        if (isDebug(anchors, root_id)) {
            LOG.info(debugIdStr + stream + " to " + out_tasks + ":" + tuple.toString());
        }

        int num_out_tasks = out_tasks.size();

        taskStats.send_tuple(stream, num_out_tasks);

        return out_tasks;
    }

    public Map<Object, List<MsgInfo>> getBatch(Integer outTaskId, String stream, List<MsgInfo> batch) {
        Map<Object, List<MsgInfo>> outTasks = new HashMap<Object, List<MsgInfo>>();
        outTasks.put(outTaskId, batch);

        taskStats.send_tuple(stream, batch.size());

        return outTasks;
    }

    public Map<Object, List<MsgInfo>> getBatch(String stream, List<MsgInfo> batch) {
        Map<Object, List<MsgInfo>> outTasks = new HashMap<Object, List<MsgInfo>>();

        // get grouper, then get which task should tuple be sent to.
        Map<String, MkGrouper> componentCrouping = streamComponentgrouper.get(stream);
        if (componentCrouping == null) {
            // if the target component's parallelism is 0, don't need send to
            // them
            LOG.debug("Failed to get Grouper of " + stream + " in " + debugIdStr);
            return outTasks;
        }

        for (Entry<String, MkGrouper> ee : componentCrouping.entrySet()) {
            MkGrouper g = ee.getValue();

            if (GrouperType.direct.equals(g.gettype())) {
                throw new IllegalArgumentException("Cannot do regular emit to direct stream");
            }

            outTasks.putAll(g.grouperBatch(batch));
        }

        /*
        if (isDebug(anchors, root_id)) {

            LOG.info(debugIdStr + stream + " to " + out_tasks);
        }*/

        int num_out_tasks = 0;
        for (Entry<Object, List<MsgInfo>> entry : outTasks.entrySet()) {
            if (entry.getKey() instanceof Integer) {
            	num_out_tasks += entry.getValue().size();
            } else {
                num_out_tasks += ((List<Integer>) entry.getKey()).size() * entry.getValue().size();
            }
        }
        taskStats.send_tuple(stream, num_out_tasks);

        return outTasks;
    }
    
    public void updateStreamCompGrouper(Map<String, Map<String, MkGrouper>> streamComponentgrouper) {
        this.streamComponentgrouper = streamComponentgrouper;
    }

    private boolean isDebug(Collection<Tuple> anchors, Object root_id) {
        if (root_id != null) {
            return JStormDebugger.isDebug(root_id);
        }
        if (anchors != null) {
            return JStormDebugger.isDebug(anchors);
        }
        return false;
    }
}
