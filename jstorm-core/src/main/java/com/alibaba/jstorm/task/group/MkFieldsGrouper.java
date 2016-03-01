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

import java.util.*;

import backtype.storm.tuple.Fields;

import com.alibaba.jstorm.task.execute.MsgInfo;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * field grouping
 *
 * @author yannian
 *
 */
public class MkFieldsGrouper {
    private Fields out_fields;
    private Fields group_fields;
    private List<Integer> out_tasks;
    private Map<Integer, List<Integer>> hash_targetTasks;

    public MkFieldsGrouper(Fields _out_fields, Fields _group_fields, List<Integer> _out_tasks) {

        for (Iterator<String> it = _group_fields.iterator(); it.hasNext();) {
            String groupField = it.next();

            // if groupField isn't in _out_fields, it would throw Exception
            _out_fields.fieldIndex(groupField);
        }

        this.out_fields = _out_fields;
        this.group_fields = _group_fields;
        this.out_tasks = _out_tasks;
        this.hash_targetTasks = new HashMap<Integer, List<Integer>>();

    }

    public List<Integer> grouper(List<Object> values) {
        int hashcode = this.out_fields.select(this.group_fields, values).hashCode();
        int group = Math.abs(hashcode % this.out_tasks.size());
        return JStormUtils.mk_list(out_tasks.get(group));
    }

    public void batchGrouper(List<MsgInfo> batch, Map<List<Integer>, List<MsgInfo>> ret){
        // field grouping for performance
        Map<Integer, List<MsgInfo>> hashMsgs = new HashMap<Integer, List<MsgInfo>>();

        for (int i = 0; i < batch.size(); i++ ) {
            MsgInfo msg = batch.get(i);
            int hashcode = this.out_fields.select(this.group_fields, msg.values).hashCode();
            int group = Math.abs(hashcode % this.out_tasks.size());

            List<MsgInfo> fieldBatch = hashMsgs.get(group);
            if (fieldBatch == null) {
                fieldBatch = JStormUtils.mk_list();
                hashMsgs.put(group, fieldBatch);
            }
            fieldBatch.add(msg);
        }

        for (Map.Entry<Integer, List<MsgInfo>> entry : hashMsgs.entrySet()){
            List<Integer> tasks = hash_targetTasks.get(entry.getKey());
            if (tasks == null){
                tasks = JStormUtils.mk_list(out_tasks.get(entry.getKey()));
                hash_targetTasks.put(entry.getKey(), tasks);
            }
            ret.put(tasks, entry.getValue());
        }
    }
}
