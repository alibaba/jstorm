package com.alibaba.jstorm.task.master.ctrlevent;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMasterContext;
import com.alibaba.jstorm.transactional.state.SnapshotStateMaster;

import backtype.storm.tuple.Tuple;

public class CtrlEventDispatcher implements TMHandler{
	private static final Logger LOG = LoggerFactory.getLogger(CtrlEventDispatcher.class);

    private SnapshotStateMaster snapshotStateMaster;
    
    private TopologyMasterContext tmContext;

    private StormClusterState zkCluster;

    private TopologyContext context;

	@Override
	public void init(TopologyMasterContext tmContext) {
		this.tmContext = tmContext;

        this.zkCluster = tmContext.getZkCluster();

        this.context = tmContext.getContext();

        this.snapshotStateMaster = new SnapshotStateMaster(tmContext.getContext(),
        		tmContext.getCollector());
        
	}

	@Override
	public void process(Object event) throws Exception {
		if (event instanceof UpdateConfigEvent) {
			update(((UpdateConfigEvent)event).getConf());
			return ;
		}
		
		
		Tuple input = (Tuple)event;
		
		TopoMasterCtrlEvent ctlEvent = (TopoMasterCtrlEvent) input.getValues().get(0);
        if (ctlEvent != null) {
          if (ctlEvent.isTransactionEvent()) {
                snapshotStateMaster.process(input);
            } else {
                String errorInfo = "Received unexpected control event, {}" + event.toString();
                LOG.warn(errorInfo);
                zkCluster.report_task_error(context.getTopologyId(), context.getThisTaskId(), errorInfo, ErrorConstants.WARN, ErrorConstants.CODE_USER);
            }
        }
	}
	
    public void update(Map conf) {
        LOG.info("Topology master received new conf:" + conf);
        
        tmContext.getConf().putAll(conf);
    }

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
