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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;

import com.alibaba.jstorm.transactional.state.IKvState;

public class WindowedKvState<K, V> implements IKvState<K, V> {
    private static final long serialVersionUID = 310177555034652880L;

    private transient IKvWindowedState<K, V> windowedStateManager;
    private TimeWindow window;
    private Map<K, V> localBatch;

    public WindowedKvState() {
        
    }

    public WindowedKvState(TimeWindow window, IKvWindowedState<K, V> windowedStateManager) {
        this.windowedStateManager = windowedStateManager;
        this.localBatch = new HashMap<K, V>();
        this.window = window;   
    }

    public void setWindowedStateManager(IKvWindowedState<K, V> windowedStateManager) {
        this.windowedStateManager = windowedStateManager;
    }

    public void checkpointBatch() {
        windowedStateManager.putBatch(window, localBatch);
        clearLocalCache();
    }

    @Override
    public void init(TopologyContext context) {
        
    }

    @Override
    public void cleanup() {
        windowedStateManager.removeWindow(window);
    }

    @Override
    public void put(K key, V value) {
        localBatch.put(key, value);
    }

    @Override
    public V get(K key) {
        V value = localBatch.get(key);
        if (value == null) {
            value = windowedStateManager.get(window, key);
        }
        return value;
    }

    @Override
    public void putBatch(Map<K, V> batch) {
        localBatch.putAll(batch);
    }

    @Override
    public Map<K, V> getBatch(Collection<K> keys) {
        Map<K, V> batch = new HashMap<>();
        batch.putAll(windowedStateManager.getBatch(window, keys));
        for (K key : keys) {
            V value = batch.get(key);
            if (value != null)
                batch.put(key, value);
        }
        return batch;
    }

    @Override
    public Collection<K> getAllKeys() {
        Set<K> keys = new HashSet<>();
        keys.addAll(localBatch.keySet());
        keys.addAll(windowedStateManager.getAllKeys(window));
        return keys;
    }

    @Override
    public Map<K, V> getBatch() {
        Map<K, V> ret = new HashMap<>();
        ret.putAll(windowedStateManager.getBatch(window));
        ret.putAll(localBatch);
        return ret;
    }

    @Override
    public void remove(K key) {
        this.remove(key);
    }

    @Override
    public void clear() {
        clearLocalCache();
        windowedStateManager.removeWindow(window);
    }

    private void clearLocalCache() {
        localBatch = new HashMap<>();
    }

    @Override
    public String toString() {
        return "[" + window.getStart() + ", " + window.getEnd() + ", Localcache=" + localBatch + "]";
    }
}