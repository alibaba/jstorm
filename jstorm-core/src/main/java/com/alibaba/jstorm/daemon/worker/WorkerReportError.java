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
package com.alibaba.jstorm.daemon.worker;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.utils.TimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;

/**
 * @author xiaojian.fxj
 */
public class WorkerReportError {
    private static Logger LOG = LoggerFactory.getLogger(WorkerReportError.class);
    private StormClusterState zkCluster;
    private String hostName;

    public WorkerReportError(StormClusterState stormClusterState, String hostName) {
        this.zkCluster = stormClusterState;
        this.hostName = hostName;
    }

    public void report(String topologyId, Integer workerPort,
                       Set<Integer> tasks, String error, int errorCode) {
        // Report worker's error to zk
        try {
            Date now = new Date();
            String nowStr = TimeFormat.getSecond(now);
            String errorInfo = error + "on " + this.hostName + ":" + workerPort + "," + nowStr;
            //we only report one task
            for (Integer task : tasks) {
                zkCluster.report_task_error(topologyId, task, errorInfo, ErrorConstants.FATAL, errorCode);
                break;
            }
        } catch (Exception e) {
            LOG.error("Failed to update errors of port " + workerPort + " to zk.", e);
        }
    }
}
