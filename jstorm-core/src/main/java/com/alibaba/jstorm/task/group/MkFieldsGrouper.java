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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

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
    private HashFunction hashFunction = Hashing.murmur3_128(17);

    public MkFieldsGrouper(Fields _out_fields, Fields _group_fields, List<Integer> _out_tasks) {

        for (Iterator<String> it = _group_fields.iterator(); it.hasNext();) {
            String groupField = it.next();

            // if groupField isn't in _out_fields, it would throw Exception
            _out_fields.fieldIndex(groupField);
        }

        this.out_fields = _out_fields;
        this.group_fields = _group_fields;
        this.out_tasks = _out_tasks;

    }

    public List<Integer> grouper(List<Object> values) {
            byte[] raw = null;
            List<Object> selectedFields = this.out_fields.select(this.group_fields, values);
            ByteBuffer out = ByteBuffer.allocate(selectedFields.size() * 4);
            for (Object o: selectedFields) {
                if (o instanceof List) {
                    out.putInt(Arrays.deepHashCode(((List) o).toArray()));
                } else if (o instanceof Object[]) {
                    out.putInt(Arrays.deepHashCode((Object[])o));
                } else if (o instanceof byte[]) {
                    out.putInt(Arrays.hashCode((byte[]) o));
                } else if (o instanceof short[]) {
                    out.putInt(Arrays.hashCode((short[]) o));
                } else if (o instanceof int[]) {
                    out.putInt(Arrays.hashCode((int[]) o));
                } else if (o instanceof long[]) {
                    out.putInt(Arrays.hashCode((long[]) o));
                } else if (o instanceof char[]) {
                    out.putInt(Arrays.hashCode((char[]) o));
                } else if (o instanceof float[]) {
                    out.putInt(Arrays.hashCode((float[]) o));
                } else if (o instanceof double[]) {
                    out.putInt(Arrays.hashCode((double[]) o));
                } else if (o instanceof boolean[]) {
                    out.putInt(Arrays.hashCode((boolean[]) o));
                } else if (o != null) {
                    out.putInt(o.hashCode());
                } else {
                    out.putInt(0);
                }
            }
        raw = out.array();
        int group = (int) (Math.abs(hashFunction.hashBytes(raw).asLong()) % this.out_tasks.size());
        return JStormUtils.mk_list(out_tasks.get(group));
    }
}
