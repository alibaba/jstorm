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
package com.alibaba.jstorm.task.master;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyMasterContext {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyMasterContext.class);

    private final Map conf;
    private final TopologyContext context;
    private final StormClusterState zkCluster;
    private final OutputCollector collector;

    private final int taskId;
    private final String topologyId;
    private final AtomicReference<Set<ResourceWorkerSlot>> workerSet;
    private final TopologyMetricContext topologyMetricContext;

    public TopologyMasterContext(Map stormConf, TopologyContext context,
                                 final OutputCollector collector) {
        this.conf = context.getStormConf();
        this.context = context;
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        this.topologyId = context.getTopologyId();
        this.zkCluster = context.getZkCluster();

        workerSet = new AtomicReference<>();
        try {
            Assignment assignment = zkCluster.assignment_info(topologyId, null);
            this.workerSet.set(assignment.getWorkers());
        } catch (Exception e) {
            LOG.error("Failed to get assignment for " + topologyId);
            throw new RuntimeException(e);
        }

        this.topologyMetricContext = new TopologyMetricContext(topologyId, workerSet.get(), conf);
    }

    public Map getConf() {
        return conf;
    }

    public TopologyContext getContext() {
        return context;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public OutputCollector getCollector() {
        return collector;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public AtomicReference<Set<ResourceWorkerSlot>> getWorkerSet() {
        return workerSet;
    }

    public TopologyMetricContext getTopologyMetricContext() {
        return topologyMetricContext;
    }


}
