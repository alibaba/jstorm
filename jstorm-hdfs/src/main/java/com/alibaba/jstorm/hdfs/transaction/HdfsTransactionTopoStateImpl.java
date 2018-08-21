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
package com.alibaba.jstorm.hdfs.transaction;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.hdfs.HdfsCache;
import com.alibaba.jstorm.transactional.state.ITopologyStateOperator;
import com.alibaba.jstorm.transactional.state.SnapshotState;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.utils.JStormUtils;

public class HdfsTransactionTopoStateImpl implements ITopologyStateOperator, Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsTransactionTopoStateImpl.class);

    private static String STATE_FILE_NAME = "topologyState";

    protected TopologyContext context;
    protected HdfsCache hdfs;
    private int RETRY_NUM;
    private int RETRY_INTERVAL;
    private String stateBasePath;
    protected Long lastSuccessBatchId = null;

    public HdfsTransactionTopoStateImpl() {

    }

    @Override
	public void init(TopologyContext context) {
	    this.context = context;
	    Map conf = context.getStormConf();
		this.hdfs = new HdfsCache(conf);
		this.stateBasePath = hdfs.getBaseDir();
		this.RETRY_NUM = JStormUtils.parseInt(conf.get("transaction.topology.state.retry.num"), 5);
		this.RETRY_INTERVAL = JStormUtils.parseInt(conf.get("transaction.topology.state.retry.interval.ms"), 100);
	}

    @Override
    public Object initState(String topologyName) {
        Object topologyState = null;
        int retry = RETRY_NUM;
        boolean success = false;

        try {
            String topologyPath = stateBasePath + "/" + topologyName;
            if (!hdfs.exist(topologyPath))
                hdfs.mkdir(topologyPath);

            String statePath = topologyPath + "/" + STATE_FILE_NAME;
            if (hdfs.exist(statePath)) {
                while (retry > 0 && !success) {
                    try {
                        topologyState = Utils.maybe_deserialize(hdfs.read(statePath));
                        success = true;
                    } catch (Exception e) {
                        LOG.warn("Failed to get init state for " + topologyName + ", retry=" + retry, e);
                        JStormUtils.sleepMs(100);
                    }
                    retry--;
                }
                if (!success) {
                    throw new RuntimeException("Failed to get init state");
                }
            }
        } catch (IOException e) {
            LOG.info("Failed to check if dst file-{} is existing. {}", topologyName + File.separator + STATE_FILE_NAME, e);
            throw new RuntimeException(e.getMessage());
        }

        // For backward compatibility.
        if (topologyState != null && topologyState instanceof Map) {
            Map state = (Map) topologyState;
            Collection values = state.values();
            if (values != null && values.size() > 0) {
                topologyState = values.iterator().next();
            } else {
                topologyState = null;
            }
        }

        if (topologyState != null) {
            SnapshotState snapshotState = ((SnapshotState) topologyState);
            lastSuccessBatchId = snapshotState.getLastSuccessfulBatchId();
            LOG.debug("spout states: {}", snapshotState.getLastSuccessfulBatch().getSpoutStates());
            LOG.debug("bolt states: {}", snapshotState.getLastSuccessfulBatch().getStatefulBoltStates());
        }

        return topologyState;
    }

    @Override
    public boolean commitState(String topologyName, Object state) {
        boolean ret = false;
        int retry = RETRY_NUM;
        while (retry > 0) {
            try {
                String statePath = stateBasePath + "/" + topologyName + "/" + STATE_FILE_NAME;
                hdfs.create(statePath, Utils.serialize(state));
                ret = true;
                break;
            } catch (Exception e) {
                LOG.warn("Failed to commit state for " + topologyName + ", retry=" + retry, e);
                JStormUtils.sleepMs(RETRY_INTERVAL);
            }
            retry--;
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        if (hdfs != null)
            hdfs.close();
    }
}