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

import java.util.Map;

import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.schedule.Assignment;

public class TaskStartEvent extends MetricEvent {
    private Assignment           oldAssignment;
    private Assignment           newAssignment;
    private Map<Integer, String> task2Component;
    
    @Override
    public void run() {
        Assignment assignment = newAssignment;
        TopologyMetricContext metricContext = context.getTopologyMetricContexts().get(topologyId);
        if (metricContext != null) {
            metricContext.setWorkerSet(assignment.getWorkers());
        } else {
            metricContext = new TopologyMetricContext();
            metricContext.setWorkerSet(assignment.getWorkers());
            context.getTopologyMetricContexts().put(topologyId, metricContext);
        }
        context.getMetricUploaderDelegate().sendEvent(context.getClusterName(), this);
    }

    public Assignment getOldAssignment() {
        return oldAssignment;
    }

    public void setOldAssignment(Assignment oldAssignment) {
        this.oldAssignment = oldAssignment;
    }

    public Assignment getNewAssignment() {
        return newAssignment;
    }

    public void setNewAssignment(Assignment newAssignment) {
        this.newAssignment = newAssignment;
    }

    public Map<Integer, String> getTask2Component() {
        return task2Component;
    }

    public void setTask2Component(Map<Integer, String> task2Component) {
        this.task2Component = task2Component;
    }
}
