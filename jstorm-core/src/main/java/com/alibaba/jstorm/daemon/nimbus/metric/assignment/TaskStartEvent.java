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
        context.getMetricUploader().sendEvent(context.getClusterName(), this);
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
