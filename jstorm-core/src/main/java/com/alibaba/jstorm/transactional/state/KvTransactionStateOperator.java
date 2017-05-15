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

/**
 * This abstract class implements the interfaces of transaction state for all key ranges of current task
 * @param <K> Key type of KV-State
 * @param <V> Value type of KV-State
 */
public abstract class KvTransactionStateOperator<K, V> extends TransactionStateOperator<K, V> {
    private static final long serialVersionUID = -7678598671209336463L;

    private static final Logger LOG = getLogger(KvTransactionStateOperator.class);

    protected KeyRangeState<K, V> keyRangeState;

    public IKvState<K, V> createState(TopologyContext context) {
        keyRangeState = new KeyRangeState<K, V>();
        keyRangeState.init(context);
        return keyRangeState;
    }

    @Override
    public void initState(Object userState) {
        Map<Integer, String> taskState = (Map<Integer, String>) userState;
        LOG.info("Begin to restore from state: {}", taskState);
        restore(taskState);
    }

    @Override
    public Object finishBatch(long batchId) {
        checkpoint(batchId, keyRangeState);
        for (Entry<Integer, ICheckpointKvState<K, V, String>> entry : keyRangeState.getAllStates().entrySet()) {
            ICheckpointKvState<K, V, String> state = entry.getValue();
            state.checkpoint(batchId);
        }
        return null;
    }

    @Override
    public Object commit(long batchId, Object userState) {
        // Map<KeyRange, RemoteCheckpointPath>
        Map<Integer, String> taskState = new HashMap<Integer, String>();
        for (Entry<Integer, ICheckpointKvState<K, V, String>> entry : keyRangeState.getAllStates().entrySet()) {
            Integer keyRange = entry.getKey();
            ICheckpointKvState<K, V, String> state = entry.getValue();
            String checkpointPath = state.backup(batchId);
            taskState.put(keyRange, checkpointPath);
        }
        return taskState;
    }

    @Override
    public void rollBack(Object userState) {
        Map<Integer, String> taskState = (Map<Integer, String>) userState;
        restore(taskState);
    }

    @Override
    public void ackCommit(long batchId, long timeStamp) {
        for (ICheckpointKvState<K, V, String> state : keyRangeState.getAllStates().values()) {
            state.remove(batchId);
        }
    }

    private void restore(Map<Integer, String> keyRangeToCheckpointPath) {
        for (Entry<Integer, ICheckpointKvState<K, V, String>> entry : keyRangeState.getAllStates().entrySet()) {
            Integer keyRange = entry.getKey();
            ICheckpointKvState<K, V, String> state = entry.getValue();
            String checkpointPath = keyRangeToCheckpointPath != null ? keyRangeToCheckpointPath.get(keyRange) : null;
            state.restore(checkpointPath);
        }
    }
}