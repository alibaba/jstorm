/*
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
package com.alibaba.jstorm.cache.memory;

import com.alibaba.jstorm.cache.IKvStore;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class InMemoryKvStore<K, V> implements IKvStore<K, V> {
    private Map<K, V> kvStore = Maps.newHashMap();

    public InMemoryKvStore() {
    }

    @Override
    public V get(K key) throws IOException {
        return kvStore.get(key);
    }

    @Override
    public Map<K, V> getBatch(Collection<K> keys) throws IOException {
        Map<K, V> batch = Maps.<K, V>newHashMap();
        for (K key : keys) {
            batch.put(key, kvStore.get(key));
        }
        return batch;
    }

    @Override
    public void put(K key, V value) throws IOException {
        kvStore.put(key, value);
    }

    @Override
    public void putBatch(Map<K, V> batch) throws IOException {
        kvStore.putAll(batch);
    }

    @Override
    public void remove(K key) throws IOException {
        kvStore.remove(key);
    }

    @Override
    public void removeBatch(Collection<K> keys) throws IOException {
        for (K key : keys) {
            kvStore.remove(key);
        }
    }

    @Override
    public Collection<K> listKeys() throws IOException {
        return kvStore.keySet();
    }

    @Override
    public void close() throws IOException {
        kvStore.clear();
    }
}