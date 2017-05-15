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
public enum MetricType {
    COUNTER("C", 1), GAUGE("G", 2), METER("M", 3), HISTOGRAM("H", 4);

    private String v;
    private int t;

    MetricType(String v, int t) {
        this.v = v;
        this.t = t;
    }

    public int getT() {
        return this.t;
    }

    public String getV() {
        return this.v;
    }

    private static final Map<String, MetricType> valueMap = new HashMap<String, MetricType>();
    private static final Map<Integer, MetricType> typeMap = new HashMap<Integer, MetricType>();

    static {
        for (MetricType type : MetricType.values()) {
            typeMap.put(type.getT(), type);
            valueMap.put(type.getV(), type);
        }
    }

    public static MetricType parse(char ch) {
        return parse(ch + "");
    }

    public static MetricType parse(String s) {
        return valueMap.get(s);
    }

    public static MetricType parse(int t) {
        return typeMap.get(t);
    }
}
