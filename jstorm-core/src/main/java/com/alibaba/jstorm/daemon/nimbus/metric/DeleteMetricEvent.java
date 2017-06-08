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
package com.alibaba.jstorm.daemon.nimbus.metric;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteMetricEvent extends MetricEvent {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private List<String> idList;
    private int metaType;

    @Override
    public void run() {
        try {
            context.deleteMetric(topologyId, metaType, idList);
        } catch (Exception ex) {
            LOG.error("Error", ex);
        }
    }

    public static void pushEvent(String topologyId, int metaType, List<String> idList) {
        DeleteMetricEvent event = new DeleteMetricEvent();
        event.topologyId = topologyId;
        event.metaType = metaType;
        event.idList = idList;

        ClusterMetricsRunnable.pushEvent(event);
    }
}
