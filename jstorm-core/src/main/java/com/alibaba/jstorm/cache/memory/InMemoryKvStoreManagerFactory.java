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

import backtype.storm.state.Serializer;
import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cache.IKvStore;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * For testing purpose, so ttl and stateful kv store are not supported.
 */
public class InMemoryKvStoreManagerFactory {
    private static Map<String, Manager> storeManangers = Maps.newHashMap();

    public static  IKvStoreManager getManager(String storeName) {
        synchronized (InMemoryKvStoreManagerFactory.class) {
            Manager instance = storeManangers.get(storeName);
            if (instance == null) {
                instance = new Manager();
                storeManangers.put(storeName, instance);
            }
            return instance;
        }
    }

    private static class Manager implements IKvStoreManager<Object> {
        private Map<String, IKvStore> idToKvStore;

        public Manager() {
            idToKvStore = Maps.newHashMap();
        }

        @Override
        public <K, V> IKvStore<K, V> getOrCreate(String kvStoreId) throws IOException {
            IKvStore<K, V> kvStore = null;
            synchronized (idToKvStore) {
                if (idToKvStore.containsKey(kvStoreId)) {
                    kvStore = idToKvStore.get(kvStoreId);
                } else {
                    kvStore = new InMemoryKvStore<K, V>();
                    idToKvStore.put(kvStoreId, kvStore);
                }
            }
            return kvStore;
        }

        public void close() {
            idToKvStore.clear();
        }

        @Override
        public void restore(Object checkpoint) {
            // Do nothing
        }

        @Override
        public String backup(long checkpointId) {
            // Do nothing
            return null;
        }

        @Override
        public void checkpoint(long checkpointId) {
            // Do nothing
        }

        @Override
        public void remove(long checkpointId) {
            // Do nothing
        }

        @Override
        public void initMonitor(TopologyContext context) {
            // Do nothing
        }
    }
}