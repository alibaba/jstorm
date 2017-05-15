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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

public class KeyRangeState<K, V> implements IKvState<K, V> {
    private static final long serialVersionUID = -4560531109550794317L;

    private static final Logger LOG = LoggerFactory.getLogger(KeyRangeState.class);

    private TopologyContext context;

    private int keyRangeNum;
    private Map<Integer, ICheckpointKvState<K, V, String>> keyRangeToState;

    public KeyRangeState() {
        keyRangeToState = new HashMap<Integer, ICheckpointKvState<K, V, String>>();
    }

    private void initKeyRangeState(int keyRange) {
        IRichCheckpointKvState<K, V, String> state =
                (IRichCheckpointKvState<K, V, String>) Utils.newInstance("com.alibaba.jstorm.hdfs.transaction.RocksDbHdfsState");
        state.setStateName(context.getThisComponentId() + "/" + String.valueOf(keyRange));
        state.init(context);
        keyRangeToState.put(keyRange, state);
    }

    public static Collection<Integer> keyRangesByTaskIndex(int keyRangeNum, int componentTaskNum, int taskIndex) {
        Set<Integer> keyRanges = new HashSet<Integer>();
        for (int i = 0; i <= (keyRangeNum / componentTaskNum); i++) {
            int keyRange = taskIndex + (i * componentTaskNum);
            if (keyRange < keyRangeNum)
                keyRanges.add(keyRange);
        }
        return keyRanges;
    }

    @Override
    public void init(TopologyContext context) {
        this.context = context;
        int componentTaskNum = context.getComponentTasks(context.getThisComponentId()).size();
        this.keyRangeNum = JStormUtils.getScaleOutNum(ConfigExtension.getKeyRangeNum(context.getStormConf()), componentTaskNum);
        Collection<Integer> keyRanges = keyRangesByTaskIndex(keyRangeNum, componentTaskNum, context.getThisTaskIndex());
        for (Integer keyRange : keyRanges) {
            initKeyRangeState(keyRange);
        }
        LOG.info("Finish KeyRangeState init for task-{} with key range: {}", context.getThisTaskId(), keyRanges);
    }

    @Override
    public void cleanup() {
        Iterator<Entry<Integer, ICheckpointKvState<K, V, String>>> itr = keyRangeToState.entrySet().iterator();
        while (itr.hasNext()) {
            Entry<Integer, ICheckpointKvState<K, V, String>> entry = itr.next();
            IKvState<K, V> state = entry.getValue();
            if (state != null)
                state.cleanup();
        }
    }

    @Override
    public void put(K key, V value) {
        IKvState<K, V> state = getRangeStateByKey(key);
        state.put(key, value);
    }

    @Override
    public V get(K key) {
        IKvState<K, V> state = getRangeStateByKey(key);
        return state.get(key);
    }

    @Override
    public void putBatch(Map<K, V> batch) {
        // Map<KeyRange, SubBatch>
        Map<Integer, Map<K, V>> classifyBatch = classifyBatch(batch);
        for (Entry<Integer, Map<K, V>> entry : classifyBatch.entrySet()) {
            Map<K, V> subBatch = entry.getValue();
            if (subBatch != null && subBatch.size() > 0) {
                IKvState<K, V> state = keyRangeToState.get(entry.getKey());
                state.putBatch(subBatch);
            }
        }
    }

    @Override
    public Map<K, V> getBatch(Collection<K> keys) {
        Collection<ICheckpointKvState<K, V, String>> states = keyRangeToState.values();
        Map<K, V> ret = new HashMap<K, V>();
        for (IKvState<K, V> state : states) {
            ret.putAll(state.getBatch(keys));
        }
        return ret;
    }

    @Override
    public Collection<K> getAllKeys() {
        Set<K> allKeys = new HashSet<K>();
        Collection<ICheckpointKvState<K, V, String>> states = keyRangeToState.values();
        for (IKvState<K, V> state : states) {
            Collection<K> keys = state.getAllKeys();
            if (keys != null)
                allKeys.addAll(keys);
        }
        return allKeys;
    }

    @Override
    public Map<K, V> getBatch() {
        Map<K, V> batches = new HashMap<K, V>();
        Collection<ICheckpointKvState<K, V, String>> states = keyRangeToState.values();
        for (IKvState<K, V> state : states) {
            Map<K, V> batch = state.getBatch();
            if (batch != null)
                batches.putAll(batch);
        }
        return batches;
    }

    @Override
    public void remove(K key) {
        IKvState<K, V> state = getRangeStateByKey(key);
        state.remove(key);
    }

    @Override
    public void clear() {
        Collection<ICheckpointKvState<K, V, String>> states = keyRangeToState.values();
        for (IKvState<K, V> state : states) {
            state.clear();
        }
    }

    public Map<Integer, ICheckpointKvState<K, V, String>> getAllStates() {
        return keyRangeToState;
    }

    /**
     * classify batch into several sub-batches for each key range
     * 
     * @param batch covers several key range
     * @return sub-batches which map to different key range
     */
    private Map<Integer, Map<K, V>> classifyBatch(Map<K, V> batch) {
        Map<Integer, Map<K, V>> classifiedBatch = new HashMap<Integer, Map<K, V>>();
        for (Entry<K, V> entry : batch.entrySet()) {
            int keyRange = hash(entry.getKey());
            Map<K, V> subBatch = classifiedBatch.get(keyRange);
            if (subBatch == null) {
                subBatch = new HashMap<K, V>();
                classifiedBatch.put(keyRange, subBatch);
            }
            subBatch.put(entry.getKey(), entry.getValue());
        }
        return classifiedBatch;
    }

    public IKvState<K, V> getRangeStateByKey(Object key) {
        int keyRange = hash(key);
        IKvState<K, V> state = keyRangeToState.get(keyRange);
        return state;
    }

    private int hash(Object key) {
        return Math.abs(key.hashCode() % keyRangeNum);
    }
}