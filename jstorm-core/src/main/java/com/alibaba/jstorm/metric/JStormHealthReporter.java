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
package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.health.HealthCheck.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * used for reporting queue full error to zk
 *
 * @author Jark Wu (wuchong.wc@alibaba-inc.com)
 */
public class JStormHealthReporter extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(JStormHealthReporter.class);
    private static final int THREAD_CYCLE = 60;   //report every minute
    private WorkerData workerData;

    public JStormHealthReporter(WorkerData workerData) {
        this.workerData = workerData;
    }

    @Override
    public void run() {
        StormClusterState clusterState = workerData.getZkCluster();
        String topologyId = workerData.getTopologyId();

        Map<Integer, HealthCheckRegistry> taskHealthCheckMap = JStormHealthCheck.getTaskhealthcheckmap();
        int cnt = 0;
        for (Map.Entry<Integer, HealthCheckRegistry> entry : taskHealthCheckMap.entrySet()) {
            Integer taskId = entry.getKey();
            Map<String, Result> results = entry.getValue().runHealthChecks();

            for (Map.Entry<String, Result> result : results.entrySet()) {
                if (!result.getValue().isHealthy()) {
                    try {
                        clusterState.report_task_error(topologyId, taskId, result.getValue().getMessage(),
                                ErrorConstants.WARN, ErrorConstants.CODE_QUEUE_FULL, ErrorConstants.DURATION_SECS_QUEUE_FULL);
                        cnt++;
                    } catch (Exception e) {
                        LOG.error("Failed to update health data in ZK for topo-{} task-{}.", topologyId, taskId, e);
                    }
                }
            }
        }

        if (cnt > 0) {
            LOG.info("Successfully updated {} health data to ZK for topology:{}", cnt, topologyId);
        }
    }

    @Override
    public Object getResult() {
        return THREAD_CYCLE;
    }

    @Override
    public String getThreadName() {
        return "HealthReporterThread";
    }
}
