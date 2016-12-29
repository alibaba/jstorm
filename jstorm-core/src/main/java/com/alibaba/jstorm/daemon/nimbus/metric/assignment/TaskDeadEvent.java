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
        // TODO Auto-generated method stub
        context.getMetricUploader().sendEvent(context.getClusterName(), this);
        
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
