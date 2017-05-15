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
package com.alibaba.jstorm.metric;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public enum MetaType {
    TASK(1, "T"), COMPONENT(2, "C"), STREAM(3, "S"), WORKER(4, "W"), TOPOLOGY(5, "P"), NETTY(6, "N"), NIMBUS(7, "M"),
    COMPONENT_STREAM(8, "O");

    private int t;
    private String v;

    MetaType(int t, String v) {
        this.t = t;
        this.v = v;
    }

    private static final Map<String, MetaType> valueMap = new HashMap<>();
    private static final Map<Integer, MetaType> typeMap = new HashMap<>();

    static {
        for (MetaType type : MetaType.values()) {
            typeMap.put(type.getT(), type);
            valueMap.put(type.getV(), type);
        }
    }

    public String getV() {
        return this.v;
    }

    public int getT() {
        return t;
    }

    public static MetaType parse(char ch) {
        return parse(ch + "");
    }

    public static MetaType parse(String v) {
        return valueMap.get(v);
    }

    public static MetaType parse(int t) {
        return typeMap.get(t);
    }
}
