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
package com.alibaba.jstorm.window;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import com.alibaba.jstorm.transactional.state.IKvState;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public abstract class TransactionalWindowedBolt<T extends Tuple, K, V> extends TransactionalBaseWindowBolt<T, K, V> {
    private static final long serialVersionUID = 8310112984058774752L;

    private static final Logger LOG = getLogger(TransactionalWindowedBolt.class);

    private IRichCheckpointWindowedState<K, V, String> windowedStateManager;
    private Map<TimeWindow, WindowedKvState<K, V>> windowedStates;

    // Lock to keep the concurrent correctness between purgewindow thread and execute thread
    private Lock windowUpdateLock;

    public void createState(TopologyContext context) {
        windowedStateManager = (IRichCheckpointWindowedState<K, V, String>) Utils.newInstance("com.alibaba.jstorm.hdfs.transaction.WindowedRocksDbHdfsState");
        windowedStateManager.setStateName(String.valueOf(context.getThisTaskId()));
        windowedStateManager.init(context);
        windowedStates = new HashMap<>();
        windowUpdateLock = new ReentrantLock();
    }

    public abstract void execute(T tuple, IKvState<K, V> state, TimeWindow window);
    public abstract void purgeWindow(IKvState<K, V> state, TimeWindow window);

    @Override
    public Object initWindowState(TimeWindow window) {
        WindowedKvState<K, V> windowedKvState = new WindowedKvState<>(window, windowedStateManager);
        try {
            windowUpdateLock.lock();
            windowedStates.put(window, windowedKvState);
        } finally {
            windowUpdateLock.unlock();
        }
        return windowedKvState;
    }

    @Override
    public void execute(T tuple, Object state, TimeWindow window) {
        execute(tuple, (IKvState<K, V>) state, window);
    }

    @Override
    public void purgeWindow(Object state, TimeWindow window) {
        purgeWindow((IKvState<K, V>) state, window);
        try {
            windowUpdateLock.lock();
            removeWindow(window);
        } finally {
            windowUpdateLock.unlock();
        }
    }

    @Override
    public void initState(Object userState) {
        LOG.info("Begin to restore windows from state: {}", userState);
        restore(userState);
    }

    @Override
    public Object finishBatch(long batchId) {
        try {
            windowUpdateLock.lock();
            // flush local cache states into state manager
            for (WindowedKvState<K, V> state : windowedStates.values()) {
                state.checkpointBatch();
            }
        } finally {
            windowUpdateLock.unlock();
        }
        // checkpoint all states
        windowedStateManager.checkpoint(batchId);
        return null;
    }

    @Override
    public Object commit(long batchId, Object userState) {
        String checkpointPath = windowedStateManager.backup(batchId);
        List<Object> userStates = new ArrayList<>();
        userStates.add(checkpointPath);
        userStates.add(windowedStates);
        return userStates;
    }

    @Override
    public void rollBack(Object userState) {
        LOG.info("Begin to restore windows from state: {}", userState);
        restore(userState);
    }

    @Override
    public void ackCommit(long batchId, long timeStamp) {
        windowedStateManager.remove(batchId);
    }

    private void removeWindow(TimeWindow window) {
        windowedStateManager.removeWindow(window);
        windowedStates.remove(window);
    }

    private void restore(Object state) {
        String checkpointPath = null;
        if (state != null) {
            List<Object> stateList = (List<Object>) state;
            checkpointPath = (String) stateList.get(0);

            try {
                windowUpdateLock.lock();
                windowedStates.clear();
                if (stateList.get(1) != null) {
                    windowedStates = (Map<TimeWindow, WindowedKvState<K, V>>) stateList.get(1);
                    for (WindowedKvState<K, V> window : windowedStates.values()) {
                        window.setWindowedStateManager(windowedStateManager);
                    }
                }
            } finally {
                windowUpdateLock.unlock();
            }
        }
        windowedStateManager.restore(checkpointPath);
    }

    
}