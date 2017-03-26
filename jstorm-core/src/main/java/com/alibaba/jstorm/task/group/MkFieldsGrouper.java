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
    private List<Integer> groupFieldIndex;
    private List<Integer> out_tasks;

    public MkFieldsGrouper(Fields _out_fields, Fields _group_fields, List<Integer> _out_tasks) {

        for (Iterator<String> it = _group_fields.iterator(); it.hasNext();) {
            String groupField = it.next();

            // if groupField isn't in _out_fields, it would throw Exception
            _out_fields.fieldIndex(groupField);
        }

        this.out_fields = _out_fields;
        this.group_fields = _group_fields;
        this.groupFieldIndex = new ArrayList<Integer>();
        for (String fieldStr : group_fields.toList()) {
            groupFieldIndex.add(out_fields.fieldIndex(fieldStr));
        }
        this.out_tasks = _out_tasks;
    }

    public List<Integer> grouper(List<Object> values) {
        int hashcode = getHashCode(values);
        int group = Math.abs(hashcode % this.out_tasks.size());
        return JStormUtils.mk_list(out_tasks.get(group));
    }

    public void batchGrouper(List<MsgInfo> batch, Map<Object, List<MsgInfo>> ret){
    	for (MsgInfo msg : batch) {
    		int hashcode = getHashCode(msg.values);
            int target = out_tasks.get(Math.abs(hashcode % this.out_tasks.size()));
    		List<MsgInfo> targetBatch = ret.get(target);
    		if (targetBatch == null) {
    			targetBatch = new ArrayList<MsgInfo>();
    			ret.put(target, targetBatch);
    		}
    		targetBatch.add(msg);
    	}
    }

    private int getHashCode(List<Object> tuple) {
        if (groupFieldIndex.size() == 1) {
            return tuple.get(groupFieldIndex.get(0)).hashCode();
        } else {
            List<Object> groupFieldValues = new ArrayList<Object>(group_fields.size());
            for (Integer index : groupFieldIndex) {
                groupFieldValues.add(tuple.get(index));
            }
            return groupFieldValues.hashCode();
        }
    }
}
