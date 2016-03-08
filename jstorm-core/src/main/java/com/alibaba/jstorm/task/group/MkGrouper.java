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
 * 
 */
public class MkGrouper {
    private static final Logger LOG = LoggerFactory.getLogger(MkGrouper.class);

    private TopologyContext topology_context;
    // this component output fields
    private Fields out_fields;
    private Grouping thrift_grouping;
    private Grouping._Fields fields;
    private GrouperType grouptype;
    private List<Integer> out_tasks;
    private List<Integer> local_tasks;
    private String streamId;

    // grouping method
    private RandomRange randomrange;
    private Random random;
    private MkShuffer shuffer;
    private MkCustomGrouper custom_grouper;
    private MkFieldsGrouper fields_grouper;
    private MkLocalShuffer local_shuffer_grouper;
    private MkLocalFirst localFirst;

    public MkGrouper(TopologyContext _topology_context, Fields _out_fields, Grouping _thrift_grouping, List<Integer> _outTasks, String streamId,
            WorkerData workerData) {
        this.topology_context = _topology_context;
        this.out_fields = _out_fields;
        this.thrift_grouping = _thrift_grouping;
        this.streamId = streamId;

        this.out_tasks = new ArrayList<Integer>();
        this.out_tasks.addAll(_outTasks);
        Collections.sort(this.out_tasks);

        this.local_tasks = _topology_context.getThisWorkerTasks();
        this.fields = Thrift.groupingType(thrift_grouping);
        this.grouptype = this.parseGroupType(workerData);

        String id = _topology_context.getThisTaskId() + ":" + streamId;
        LOG.info(id + " grouptype is " + grouptype + ", out_tasks is " + out_tasks + ", local_tasks" + local_tasks);

    }

    public GrouperType gettype() {
        return grouptype;
    }

    private GrouperType parseGroupType(WorkerData workerData) {

        GrouperType grouperType = null;

        if (Grouping._Fields.FIELDS.equals(fields)) {

            if (Thrift.isGlobalGrouping(thrift_grouping)) {

                // global grouping, just send tuple to first task
                grouperType = GrouperType.global;
            } else {

                List<String> fields_group = Thrift.fieldGrouping(thrift_grouping);
                Fields fields = new Fields(fields_group);

                fields_grouper = new MkFieldsGrouper(out_fields, fields, out_tasks);

                // hashcode by fields
                grouperType = GrouperType.fields;
            }

        } else if (Grouping._Fields.ALL.equals(fields)) {
            // send to every task
            grouperType = GrouperType.all;
        } else if (Grouping._Fields.SHUFFLE.equals(fields)) {
            grouperType = GrouperType.shuffle;
            shuffer = new MkShuffer(out_tasks, workerData);
        } else if (Grouping._Fields.NONE.equals(fields)) {
            // random send one task
            this.random = new Random();
            grouperType = GrouperType.none;
        } else if (Grouping._Fields.CUSTOM_OBJECT.equals(fields)) {
            // user custom grouping by JavaObject
            JavaObject jobj = thrift_grouping.get_custom_object();
            CustomStreamGrouping g = Thrift.instantiateJavaObject(jobj);
            int myTaskId = topology_context.getThisTaskId();
            String componentId = topology_context.getComponentId(myTaskId);
            GlobalStreamId stream = new GlobalStreamId(componentId, streamId);
            custom_grouper = new MkCustomGrouper(topology_context, g, stream, out_tasks, myTaskId);
            grouperType = GrouperType.custom_obj;
        } else if (Grouping._Fields.CUSTOM_SERIALIZED.equals(fields)) {
            // user custom group by serialized Object
            byte[] obj = thrift_grouping.get_custom_serialized();
            CustomStreamGrouping g = (CustomStreamGrouping) Utils.javaDeserialize(obj);
            int myTaskId = topology_context.getThisTaskId();
            String componentId = topology_context.getComponentId(myTaskId);
            GlobalStreamId stream = new GlobalStreamId(componentId, streamId);
            custom_grouper = new MkCustomGrouper(topology_context, g, stream, out_tasks, myTaskId);
            grouperType = GrouperType.custom_serialized;
        } else if (Grouping._Fields.DIRECT.equals(fields)) {
            // directly send to a special task
            grouperType = GrouperType.direct;
        } else if (Grouping._Fields.LOCAL_OR_SHUFFLE.equals(fields)) {
            grouperType = GrouperType.local_or_shuffle;
            local_shuffer_grouper = new MkLocalShuffer(local_tasks, out_tasks, workerData);
        } else if (Grouping._Fields.LOCAL_FIRST.equals(fields)) {
            grouperType = GrouperType.localFirst;
            localFirst = new MkLocalFirst(local_tasks, out_tasks, workerData);
        }

        return grouperType;
    }

    /**
     * get which task should tuple be sent to
     * 
     * @param values
     * @return
     */
    public List<Integer> grouper(List<Object> values) {
        if (GrouperType.global.equals(grouptype)) {
            // send to task which taskId is 0
            return JStormUtils.mk_list(out_tasks.get(0));
        } else if (GrouperType.fields.equals(grouptype)) {
            // field grouping
            return fields_grouper.grouper(values);
        } else if (GrouperType.all.equals(grouptype)) {
            // send to every task
            return out_tasks;
        } else if (GrouperType.shuffle.equals(grouptype)) {
            // random, but the random is different from none
            return shuffer.grouper(values);
        } else if (GrouperType.none.equals(grouptype)) {
            int rnd = Math.abs(random.nextInt() % out_tasks.size());
            return JStormUtils.mk_list(out_tasks.get(rnd));
        } else if (GrouperType.custom_obj.equals(grouptype)) {
            return custom_grouper.grouper(values);
        } else if (GrouperType.custom_serialized.equals(grouptype)) {
            return custom_grouper.grouper(values);
        } else if (GrouperType.local_or_shuffle.equals(grouptype)) {
            return local_shuffer_grouper.grouper(values);
        } else if (GrouperType.localFirst.equals(grouptype)) {
            return localFirst.grouper(values);
        } else {
            LOG.warn("Unsupportted group type");
        }

        return new ArrayList<Integer>();
    }
    
    public Map<List<Integer>, List<MsgInfo>> grouperBatch(List<MsgInfo> batch) {
        Map<List<Integer>, List<MsgInfo>> ret = new HashMap<List<Integer>, List<MsgInfo>>();
        //optimize fieldGrouping & customGrouping
        if (GrouperType.local_or_shuffle.equals(grouptype)) {
           ret.put(local_shuffer_grouper.grouper(null), batch);
        }  else if (GrouperType.global.equals(grouptype)) {
            // send to task which taskId is 0
            ret.put(JStormUtils.mk_list(out_tasks.get(0)), batch);
        } else if (GrouperType.fields.equals(grouptype)) {
            fields_grouper.batchGrouper(batch, ret);
        } else if (GrouperType.all.equals(grouptype)) {
            // send to every task
            ret.put(out_tasks, batch);
        } else if (GrouperType.shuffle.equals(grouptype)) {
            // random, but the random is different from none
            ret.put(shuffer.grouper(null), batch);
        } else if (GrouperType.none.equals(grouptype)) {
            int rnd = Math.abs(random.nextInt() % out_tasks.size());
            ret.put(JStormUtils.mk_list(out_tasks.get(rnd)), batch);
        } else if (GrouperType.custom_obj.equals(grouptype) || GrouperType.custom_serialized.equals(grouptype)) {
            for (int i = 0; i < batch.size(); i++ ) {
                MsgInfo msg = batch.get(i);
                List<Integer> out = custom_grouper.grouper(msg.values);
                List<MsgInfo> customBatch = ret.get(out);
                if (customBatch == null) {
                    customBatch = JStormUtils.mk_list();
                    ret.put(out, customBatch);
                }
                customBatch.add(msg);
            }
        } else if (GrouperType.localFirst.equals(grouptype)) {
            ret.put(localFirst.grouper(null), batch);
        } else {
            LOG.warn("Unsupportted group type");
        }
        return ret;
    }

}
