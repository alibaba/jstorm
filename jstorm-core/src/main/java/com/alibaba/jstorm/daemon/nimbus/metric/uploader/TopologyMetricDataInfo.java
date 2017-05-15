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
package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TopologyMetricDataInfo implements Serializable {
    private static final long serialVersionUID = 1303262512351757610L;
    
    public String topologyId;
    public String type;      // "tp" for tp/comp metrics OR "task" for task/stream/worker/netty metrics
    public long   timestamp; // metrics report time
    
    public Map<String, Object> toMap() {
        Map<String, Object> ret = new HashMap<>();
        ret.put(MetricUploader.METRIC_TIME, timestamp);
        ret.put(MetricUploader.METRIC_TYPE, type);
        
        return ret;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
