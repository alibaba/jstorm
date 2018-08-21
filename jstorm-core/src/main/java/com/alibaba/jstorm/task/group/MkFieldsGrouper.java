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

import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.task.execute.MsgInfo;
import com.alibaba.jstorm.utils.JStormUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * fields grouping
 *
 * @author yannian
 */
public class MkFieldsGrouper {
    protected Fields outFields;
    protected Fields groupFields;
    protected List<Integer> groupFieldIndex;
    protected List<Integer> outTasks;

    public MkFieldsGrouper(Fields _out_fields, Fields _group_fields, List<Integer> _out_tasks) {
        for (Iterator<String> it = _group_fields.iterator(); it.hasNext(); ) {
            String groupField = it.next();
            // if groupField isn't in _out_fields, it would throw Exception
            _out_fields.fieldIndex(groupField);
        }

        this.outFields = _out_fields;
        this.groupFields = _group_fields;
        this.groupFieldIndex = new ArrayList<>();
        for (String fieldStr : groupFields.toList()) {
            groupFieldIndex.add(outFields.fieldIndex(fieldStr));
        }
        this.outTasks = _out_tasks;
    }

    public List<Integer> grouper(List<Object> values) {
        int hashcode = getHashCode(values);
        int group = Math.abs(hashcode % this.outTasks.size());
        return JStormUtils.mk_list(outTasks.get(group));
    }

    public void batchGrouper(List<MsgInfo> batch, Map<Object, List<MsgInfo>> ret) {
        for (MsgInfo msg : batch) {
            int hashcode = getHashCode(msg.values);
            int target = outTasks.get(Math.abs(hashcode % this.outTasks.size()));
            List<MsgInfo> targetBatch = ret.get(target);
            if (targetBatch == null) {
                targetBatch = new ArrayList<>();
                ret.put(target, targetBatch);
            }
            targetBatch.add(msg);
        }
    }

    protected int getHashCode(List<Object> tuple) {
        if (groupFieldIndex.size() == 1) {
            return tuple.get(groupFieldIndex.get(0)).hashCode();
        } else {
            List<Object> groupFieldValues = new ArrayList<>(groupFields.size());
            for (Integer index : groupFieldIndex) {
                groupFieldValues.add(tuple.get(index));
            }
            return groupFieldValues.hashCode();
        }
    }
}
