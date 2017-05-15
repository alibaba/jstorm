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
package com.alibaba.jstorm.daemon.nimbus.metric.assignment;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

public class TaskDeadEvent extends MetricEvent {
    private Map<Integer, ResourceWorkerSlot> deadTasks;
    
    @Override
    public void run() {
        context.getMetricUploaderDelegate().sendEvent(context.getClusterName(), this);
        
        // unregister dead workers
        Set<ResourceWorkerSlot> workers = new HashSet<>();
        workers.addAll(deadTasks.values());
        for (ResourceWorkerSlot worker : workers) {
            context.getMetricCache().unregisterWorker(topologyId, worker.getHostname(), worker.getPort());
        }
    }
    
    public Map<Integer, ResourceWorkerSlot> getDeadTasks() {
        return deadTasks;
    }
    
    public void setDeadTasks(Map<Integer, ResourceWorkerSlot> deadTasks) {
        this.deadTasks = deadTasks;
    }
    
    public static void pushEvent(String topologyId, Map<Integer, ResourceWorkerSlot> deadTasks) {
        TaskDeadEvent event = new TaskDeadEvent();
        event.topologyId = topologyId;
        event.deadTasks = deadTasks;
        
        ClusterMetricsRunnable.pushEvent(event);
    }
}
