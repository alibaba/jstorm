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
package com.alibaba.jstorm.cache;

import backtype.storm.state.Serializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cache.memory.InMemoryKvStoreManagerFactory;
import com.alibaba.jstorm.cache.rocksdb.RocksDbKvStoreManagerFactory;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.SerializerFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class KvStoreManagerFactory {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(KvStoreManagerFactory.class);

    public enum KvStoreType {
        memory("memory"), rocksdb("rocksdb");

        private String type;
        private KvStoreType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }
    }

    private static KvStoreType getKvStoreType(Map conf) {
        String type = Utils.getString(conf.get(ConfigExtension.KV_STORE_TYPE), KvStoreType.rocksdb.toString());
        return KvStoreType.valueOf(type);
    }

    public static <T> IKvStoreManager<T> getKvStoreManager(Map conf, String storeName, String storePath, boolean isStateful) throws IOException {
        return getKvStoreManager(getKvStoreType(conf), conf, storeName, storePath, isStateful);
    }

    public static <T> IKvStoreManager<T> getKvStoreManager(KvStoreType type, Map conf, String storeName, String storePath, boolean isStateful) throws IOException {
        Serializer serializer = SerializerFactory.createSerailzer(conf);
        int ttlTimeSec = ConfigExtension.getStateTtlTime(conf);
        IKvStoreManager<T> kvStoreManager = null;
        switch(type) {
            case memory:
                kvStoreManager = InMemoryKvStoreManagerFactory.getManager(storeName);
                break;
            case rocksdb:
                if (isStateful) {
                    kvStoreManager = ttlTimeSec > 0 ?
                            RocksDbKvStoreManagerFactory.getStatefulManager(conf, storeName, storePath, serializer, ttlTimeSec) :
                            RocksDbKvStoreManagerFactory.getStatefulManager(conf, storeName, storePath, serializer);
                } else {
                    kvStoreManager = ttlTimeSec > 0 ?
                            RocksDbKvStoreManagerFactory.getManager(conf, storeName, storePath, serializer, ttlTimeSec) :
                            RocksDbKvStoreManagerFactory.getManager(conf, storeName, storePath, serializer);
                }
                break;
            default:
                LOG.error("Unknown kv store type: {}", type.toString());
                throw new UnsupportedOperationException();
        }
        return kvStoreManager;
    }

    public static <T> IKvStoreManager<T> getKvStoreManagerWithMonitor(TopologyContext context, String storeName, String storePath, boolean isStateful) throws IOException {
        Map conf = context.getStormConf();
        return getKvStoreManagerWithMonitor(getKvStoreType(conf), context, storeName, storePath, isStateful);
    }

    public static <T> IKvStoreManager<T> getKvStoreManagerWithMonitor(
            KvStoreType type, TopologyContext context, String storeName, String storePath, boolean isStateful) throws IOException {
        IKvStoreManager<T> kvStoreManager = getKvStoreManager(type, context.getStormConf(), storeName, storePath, isStateful);
        kvStoreManager.initMonitor(context);
        return kvStoreManager;
    }
}