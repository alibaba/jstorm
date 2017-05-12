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
package com.alibaba.jstorm.task.group;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.JavaObject;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.task.execute.MsgInfo;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;
import com.alibaba.jstorm.utils.Thrift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Grouper, get which task should be send to for one tuple
 *
 * @author yannian
 */
public class MkGrouper {
    private static final Logger LOG = LoggerFactory.getLogger(MkGrouper.class);

    private TopologyContext topologyContext;
    // this component output fields
    private Fields outFields;
    private Grouping thriftGrouping;
    private Grouping._Fields fields;
    private GrouperType groupType;
    private List<Integer> outTasks;
    private List<Integer> localTasks;
    private String streamId;
    private String targetComponent;

    // grouping method
    private RandomRange randomrange;
    private Random random;
    private MkShuffer shuffer;
    private MkCustomGrouper customGrouper;
    private MkFieldsGrouper fieldsGrouper;

    public MkGrouper(TopologyContext _topology_context, Fields _out_fields, Grouping _thrift_grouping,
                     String targetComponent, String streamId, WorkerData workerData) {
        this.topologyContext = _topology_context;
        this.outFields = _out_fields;
        this.thriftGrouping = _thrift_grouping;
        this.streamId = streamId;
        this.targetComponent = targetComponent;

        List<Integer> outTasks = topologyContext.getComponentTasks(targetComponent);
        this.outTasks = new ArrayList<>();
        this.outTasks.addAll(outTasks);
        Collections.sort(this.outTasks);

        this.localTasks = _topology_context.getThisWorkerTasks();
        this.fields = Thrift.groupingType(thriftGrouping);
        this.groupType = this.parseGroupType(workerData);

        String id = _topology_context.getThisTaskId() + ":" + streamId;
        LOG.info(id + " groupType is " + groupType + ", outTasks is " + this.outTasks + ", localTasks" + localTasks);

    }

    public GrouperType gettype() {
        return groupType;
    }

    private GrouperType parseGroupType(WorkerData workerData) {
        GrouperType grouperType = null;
        if (Grouping._Fields.FIELDS.equals(fields)) {
            if (Thrift.isGlobalGrouping(thriftGrouping)) {
                // global grouping, just send tuple to first task
                grouperType = GrouperType.global;
            } else {
                List<String> fields_group = Thrift.fieldGrouping(thriftGrouping);
                Fields fields = new Fields(fields_group);
                Map conf = topologyContext.getStormConf();
                boolean enableKeyRangeHash = ConfigExtension.isEnableKeyRangeFieldGroup(conf);
                if (enableKeyRangeHash) 
                    fieldsGrouper = new MkKeyRangeFieldsGrouper(conf, outFields, fields, outTasks);
                else
                    fieldsGrouper = new MkFieldsGrouper(outFields, fields, outTasks);
                // hashcode by fields
                grouperType = GrouperType.fields;
            }
        } else if (Grouping._Fields.ALL.equals(fields)) {
            // send to every task
            grouperType = GrouperType.all;
        } else if (Grouping._Fields.SHUFFLE.equals(fields)) {
            grouperType = GrouperType.shuffle;
            shuffer = new MkShuffer(topologyContext.getThisComponentId(), targetComponent, workerData);
        } else if (Grouping._Fields.NONE.equals(fields)) {
            // random send one task
            this.random = new Random();
            grouperType = GrouperType.none;
        } else if (Grouping._Fields.CUSTOM_OBJECT.equals(fields)) {
            // user custom grouping by JavaObject
            JavaObject jobj = thriftGrouping.get_custom_object();
            CustomStreamGrouping g = Thrift.instantiateJavaObject(jobj);
            int myTaskId = topologyContext.getThisTaskId();
            String componentId = topologyContext.getComponentId(myTaskId);
            GlobalStreamId stream = new GlobalStreamId(componentId, streamId);
            customGrouper = new MkCustomGrouper(topologyContext, g, stream, outTasks, myTaskId);
            grouperType = GrouperType.custom_obj;
        } else if (Grouping._Fields.CUSTOM_SERIALIZED.equals(fields)) {
            // user custom group by serialized Object
            byte[] obj = thriftGrouping.get_custom_serialized();
            CustomStreamGrouping g = (CustomStreamGrouping) Utils.javaDeserialize(obj);
            int myTaskId = topologyContext.getThisTaskId();
            String componentId = topologyContext.getComponentId(myTaskId);
            GlobalStreamId stream = new GlobalStreamId(componentId, streamId);
            customGrouper = new MkCustomGrouper(topologyContext, g, stream, outTasks, myTaskId);
            grouperType = GrouperType.custom_serialized;
        } else if (Grouping._Fields.DIRECT.equals(fields)) {
            // directly send to a special task
            grouperType = GrouperType.direct;
        } else if (Grouping._Fields.LOCAL_OR_SHUFFLE.equals(fields)) {
            grouperType = GrouperType.shuffle;
            shuffer = new MkShuffer(topologyContext.getThisComponentId(), targetComponent, workerData);
        } else if (Grouping._Fields.LOCAL_FIRST.equals(fields)) {
            grouperType = GrouperType.shuffle;
            shuffer = new MkShuffer(topologyContext.getThisComponentId(), targetComponent, workerData);
        }

        return grouperType;
    }

    /**
     * get which task should tuple be sent to
     */
    public List<Integer> grouper(List<Object> values) {
        if (GrouperType.global.equals(groupType)) {
            // send to task which taskId is 0
            return JStormUtils.mk_list(outTasks.get(0));
        } else if (GrouperType.fields.equals(groupType)) {
            // field grouping
            return fieldsGrouper.grouper(values);
        } else if (GrouperType.all.equals(groupType)) {
            // send to every task
            return outTasks;
        } else if (GrouperType.shuffle.equals(groupType)) {
            // random, but the random is different from none
            return shuffer.grouper(values);
        } else if (GrouperType.none.equals(groupType)) {
            int rnd = Math.abs(random.nextInt() % outTasks.size());
            return JStormUtils.mk_list(outTasks.get(rnd));
        } else if (GrouperType.custom_obj.equals(groupType)) {
            return customGrouper.grouper(values);
        } else if (GrouperType.custom_serialized.equals(groupType)) {
            return customGrouper.grouper(values);
        } else {
            LOG.warn("Unsupported group type");
        }

        return new ArrayList<>();
    }

    public Map<Object, List<MsgInfo>> grouperBatch(List<MsgInfo> batch) {
        Map<Object, List<MsgInfo>> ret = new HashMap<>();
        //optimize fieldGrouping & customGrouping
        if (GrouperType.shuffle.equals(groupType)) {
            // random, but the random is different from none
            ret.put(shuffer.grouper(null), batch);
        } else if (GrouperType.global.equals(groupType)) {
            // send to task which taskId is 0
            ret.put(JStormUtils.mk_list(outTasks.get(0)), batch);
        } else if (GrouperType.fields.equals(groupType)) {
            fieldsGrouper.batchGrouper(batch, ret);
        } else if (GrouperType.all.equals(groupType)) {
            // send to every task
            ret.put(outTasks, batch);
        } else if (GrouperType.none.equals(groupType)) {
            int rnd = Math.abs(random.nextInt() % outTasks.size());
            ret.put(JStormUtils.mk_list(outTasks.get(rnd)), batch);
        } else if (GrouperType.custom_obj.equals(groupType) || GrouperType.custom_serialized.equals(groupType)) {
            for (int i = 0; i < batch.size(); i++) {
                MsgInfo msg = batch.get(i);
                List<Integer> out = customGrouper.grouper(msg.values);
                List<MsgInfo> customBatch = ret.get(out);
                if (customBatch == null) {
                    customBatch = JStormUtils.mk_list();
                    ret.put(out, customBatch);
                }
                customBatch.add(msg);
            }
        } else {
            LOG.warn("Unsupported group type");
        }
        return ret;
    }

}
