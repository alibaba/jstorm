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
package com.alibaba.jstorm.transactional.state;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;

import com.alibaba.jstorm.transactional.state.task.KeyRangeStateTaskInit;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

/**
 * This abstract class implements the interfaces of transaction state for current task
 * @param <K> Key type of KV-State
 * @param <V> Value type of KV-State
 */
public abstract class TransactionStateOperator<K, V> implements ITransactionStateOperator {
    private static final long serialVersionUID = 2545142551840944928L;

    private static final Logger LOG = getLogger(TransactionStateOperator.class);

    private IRichCheckpointKvState<K, V, String> stateInstance;

    public IKvState<K, V> createState(TopologyContext context) {
        stateInstance = (IRichCheckpointKvState<K, V, String>) Utils.newInstance("com.alibaba.jstorm.hdfs.transaction.RocksDbHdfsState");
        stateInstance.setStateName(String.valueOf(context.getThisTaskId()));
        stateInstance.init(context);
        return stateInstance;
    }

    @Override
    public void initState(Object userState) {
        LOG.info("Begin to restore from state: {}", userState);
        restore((String) userState);
    }

    public abstract void checkpoint(long batchId, IKvState<K, V> state);

    @Override
    public Object finishBatch(long batchId) {
        checkpoint(batchId, stateInstance);
        stateInstance.checkpoint(batchId);
        return null;
    }

    @Override
    public Object commit(long batchId, Object userState) {
        String checkpointPath = stateInstance.backup(batchId);
        return checkpointPath;
    }

    @Override
    public void rollBack(Object userState) {
        restore((String) userState);
    }

    @Override
    public void ackCommit(long batchId, long timeStamp) {
        stateInstance.remove(batchId);
    }

    private void restore(String checkpointPath) {
        stateInstance.restore(checkpointPath);
    }
}