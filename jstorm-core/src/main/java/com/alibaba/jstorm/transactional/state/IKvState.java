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
import java.util.Map;

public interface IKvState<K, V> extends IState {

    /**
     * put the state of key-value structure
     * @param key
     * @param value
     */
    public void put(K key, V value);

    /**
     * get the value of key-value state
     * @param key
     * @return
     */
    public V get(K key);

    /**
     * put a batch into K-V state
     * @param batch
     */
    public void putBatch(Map<K, V> batch);

    /**
     * get the state for the specified keys
     * @param keys
     * @return
     */
    public Map<K, V> getBatch(Collection<K> keys);

    /**
     * get all keys of this k-V state
     * @return
     */
    public Collection<K> getAllKeys();

    /**
     * Get all state data
     * @return
     */
    public Map<K, V> getBatch();

    /**
     * remove a record by the specified key
     * @param key
     */
    public void remove(K key);

    /**
     * remove all records
     */
    public void clear();
}