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
package com.alibaba.jstorm.task.master.ctrlevent;

import java.util.Map;

import backtype.storm.task.TopologyContext;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMasterContext;
import com.alibaba.jstorm.transactional.state.SnapshotStateMaster;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.tuple.Tuple;

public class CtrlEventDispatcher implements TMHandler {
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
        boolean isTransaction = JStormUtils.parseBoolean(tmContext.getConf().get(ConfigExtension.TRANSACTION_TOPOLOGY), false);
        if (isTransaction)
            this.snapshotStateMaster = new SnapshotStateMaster(tmContext.getContext(), tmContext.getCollector());
    }

    @Override
    public void process(Object event) throws Exception {
        if (event instanceof UpdateConfigEvent) {
            update(((UpdateConfigEvent) event).getConf());
            return;
        }

        Tuple input = (Tuple) event;

        TopoMasterCtrlEvent ctlEvent = (TopoMasterCtrlEvent) input.getValues().get(0);
        if (ctlEvent != null) {
            if (ctlEvent.isTransactionEvent()) {
                snapshotStateMaster.process(input);
            } else {
                String errorInfo = "Received unexpected control event, {}" + event.toString();
                LOG.warn(errorInfo);
                zkCluster.report_task_error(context.getTopologyId(), context.getThisTaskId(),
                        errorInfo, ErrorConstants.WARN, ErrorConstants.CODE_USER);
            }
        }
    }

    public void update(Map conf) {
        LOG.info("Topology master received new conf:" + conf);
        tmContext.getConf().putAll(conf);
    }

    @Override
    public void cleanup() {
        if (snapshotStateMaster != null)
            snapshotStateMaster.close();
    }
}
