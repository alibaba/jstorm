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
package com.alibaba.jstorm.daemon.worker.hearbeat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.WorkerHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * worker heartbeat thread
 *
 * @author yannian/Longda
 */
public class WorkerHeartbeatRunable extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(WorkerHeartbeatRunable.class);

    @SuppressWarnings("unused")
    private WorkerData workerData;

    private Map<Object, Object> conf;
    private String workerId;
    private Integer port;
    private String topologyId;
    private CopyOnWriteArraySet<Integer> taskIds;
    private Integer frequency;
    private Map<String, LocalState> workerStates;

    public WorkerHeartbeatRunable(WorkerData workerData) {
        this.workerData = workerData;

        this.conf = workerData.getStormConf();
        this.workerId = workerData.getWorkerId();
        this.port = workerData.getPort();
        this.topologyId = workerData.getTopologyId();
        this.taskIds = new CopyOnWriteArraySet<>(workerData.getTaskIds());

        String key = Config.WORKER_HEARTBEAT_FREQUENCY_SECS;
        frequency = JStormUtils.parseInt(conf.get(key), 10);

        this.workerStates = new HashMap<>();
    }

    private LocalState getWorkerState() throws IOException {
        LocalState state = workerStates.get(workerId);
        if (state == null) {
            state = StormConfig.worker_state(conf, workerId);
            workerStates.put(workerId, state);
        }
        return state;
    }

    /**
     * do heartbeat and update LocalState
     */

    public void doHeartbeat() throws IOException {
        int curTime = TimeUtils.current_time_secs();
        WorkerHeartbeat hb = new WorkerHeartbeat(curTime, topologyId, taskIds, port);

        LOG.debug("Doing heartbeat:" + workerId + ",port:" + port + ",hb" + hb.toString());
        LocalState state = getWorkerState();
        state.put(Common.LS_WORKER_HEARTBEAT, hb);
    }

    @Override
    public void run() {
        try {
            doHeartbeat();
        } catch (IOException e) {
            LOG.error("worker heartbeat failed", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getResult() {
        return frequency;
    }
}
