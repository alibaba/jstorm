package com.alibaba.jstorm.task.master;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

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
        
        
        
        workerSet = new AtomicReference<Set<ResourceWorkerSlot>>();
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
