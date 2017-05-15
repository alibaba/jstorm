package com.alibaba.jstorm.daemon.worker;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.TimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * @author xiaojian.fxj
 */
public class WorkerReportError implements IWorkerReportError {
    private static Logger LOG = LoggerFactory.getLogger(WorkerReportError.class);
    private StormClusterState zkCluster;
    private String hostName;

    @Override
    public void init(Map conf, Object obj) {
        this.hostName = JStormServerUtils.getHostName(conf);
        this.zkCluster = (StormClusterState)obj;
    }

    public void report(String topologyId, Integer workerPort,
                       Set<Integer> tasks, String error, int errorCode) {
        // Report worker's error to zk
        try {
            Date now = new Date();
            String nowStr = TimeFormat.getSecond(now);
            String errorInfo = error + "on " + this.hostName + ":" + workerPort + "," + nowStr;
            for (Integer task : tasks) {
                zkCluster.report_task_error(topologyId, task, errorInfo, ErrorConstants.FATAL, errorCode);
            }
        } catch (Exception e) {
            LOG.error("Failed to update errors of port " + workerPort + " to ZK.", e);
        }
    }
}
