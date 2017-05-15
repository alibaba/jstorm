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
package com.alibaba.jstorm.transactional.state.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.transactional.state.KeyRangeState;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.utils.JStormUtils;

public class KeyRangeStateTaskInit implements ITaskStateInitOperator {
    private static final Logger LOG = getLogger(KeyRangeStateTaskInit.class);

    @Override
    public Object getTaskInitState(Map conf, int currTaskId, Set<Integer> currComponentTasks, Map<Integer, TransactionState> prevComponentTaskStates) {
        Map<Integer, String> currKeyRangeToCheckpoints = new HashMap<Integer, String>();
        Map<Integer, String> prevKeyRangeToCheckpoints = getAllKeyRangeCheckpointPaths(prevComponentTaskStates.values());
        int componentTaskNum = currComponentTasks.size();
        int taskIndex = JStormUtils.getTaskIndex(currTaskId, currComponentTasks);
        int prevKeyRangeNum = prevKeyRangeToCheckpoints.size();
        int configKeyRangeNum = ConfigExtension.getKeyRangeNum(conf);
        int currKeyRangeNum = JStormUtils.getScaleOutNum(configKeyRangeNum, componentTaskNum);
        Collection<Integer> currKeyRanges = KeyRangeState.keyRangesByTaskIndex(currKeyRangeNum, componentTaskNum, taskIndex);
        LOG.debug("currKeyRanges: {}, prevKeyRangeToCheckpoints : {}", currKeyRanges, prevKeyRangeToCheckpoints);

        // Check if scale-out of key range happens
        if (currKeyRangeNum > prevKeyRangeNum) {
            // key range number increases by 2^n times by default
            for (Integer keyRange : currKeyRanges) {
                currKeyRangeToCheckpoints.put(keyRange, prevKeyRangeToCheckpoints.get(keyRange % prevKeyRangeNum));
            }
        } else {
            for (Integer keyRange : currKeyRanges) {
                currKeyRangeToCheckpoints.put(keyRange, prevKeyRangeToCheckpoints.get(keyRange));
            }
        }
        return currKeyRangeToCheckpoints;
    }

    private Map<Integer, String> getAllKeyRangeCheckpointPaths(Collection<TransactionState> allStates) {
        Map<Integer, String> keyRangeToCheckpoints = new HashMap<Integer, String>();
        for (TransactionState state : allStates) {
            Map<Integer, String> checkpoint = (Map<Integer, String>) state.getUserCheckpoint();
            if (checkpoint != null)
                keyRangeToCheckpoints.putAll(checkpoint);
        }
        return keyRangeToCheckpoints;
        
    }
}