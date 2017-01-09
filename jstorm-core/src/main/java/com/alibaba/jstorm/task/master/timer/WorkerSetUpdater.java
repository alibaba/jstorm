package com.alibaba.jstorm.task.master.timer;

import java.util.Set;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMasterContext;

public class WorkerSetUpdater implements TMHandler{
	private static final Logger LOG = LoggerFactory.getLogger(WorkerSetUpdater.class);
	
	TopologyMasterContext tmContext;

	private StormClusterState zkCluster;
	private TopologyContext context;

	@Override
	public void init(TopologyMasterContext tmContext) {
		// TODO Auto-generated method stub
		this.tmContext = tmContext;
		this.zkCluster = tmContext.getZkCluster();
		this.context = tmContext.getContext();
	}



	@Override
	public void process(Object event) throws Exception {
		// TODO Auto-generated method stub
		try {
        	Assignment assignment = tmContext.getZkCluster().assignment_info(tmContext.getTopologyId(), null);
        	if (assignment != null) {
        		Set<ResourceWorkerSlot> oldWorkerSet = tmContext.getWorkerSet().get();
        		Set<ResourceWorkerSlot> newWorkerSet = assignment.getWorkers();
        		if (oldWorkerSet.equals(newWorkerSet) == false) {
        			LOG.info("Find worker slots has been changed, old:{}, \n\n new:{}", oldWorkerSet, newWorkerSet);
        			tmContext.getWorkerSet().set(newWorkerSet);
        			tmContext.getTopologyMetricContext().setWorkerSet(newWorkerSet);
        		}
        	}
        } catch (Exception e) {
			String errorInfo = "Failed to get assignment for " + tmContext.getTopologyId();
			LOG.error(errorInfo + e);
			zkCluster.report_task_error(context.getTopologyId(), context.getThisTaskId(), errorInfo, ErrorConstants.WARN, ErrorConstants.CODE_USER);
        }
	}



	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
