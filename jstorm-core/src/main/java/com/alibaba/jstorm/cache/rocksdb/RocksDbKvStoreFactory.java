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
package com.alibaba.jstorm.cache.rocksdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.jstorm.cache.IKvStore;
import org.rocksdb.*;

import backtype.storm.state.Serializer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory class for RocksDb implementation of {@link IKvStore}.
 */
public class RocksDbKvStoreFactory {

    /**
     * Returns a RocksDb implementation of {@link IKvStore}.
     */
    public static <K, V> IKvStore<K, V> rocksDbCache(
            RocksDB rocksDB,
            ColumnFamilyHandle columnFamily,
            Serializer<Object> serializer) throws IOException {
        return new RocksDbCacheImpl<>(rocksDB, columnFamily, serializer);
    }

    private RocksDbKvStoreFactory() {}

    private static class RocksDbCacheImpl<K, V> implements IKvStore<K, V> {
        private final RocksDB rocksDb;
        private final ColumnFamilyHandle columnFamily;
        private final Serializer<Object> serializer;

        public RocksDbCacheImpl(RocksDB rocksDb, ColumnFamilyHandle columnFamily, Serializer<Object> serializer) {
            this.rocksDb = checkNotNull(rocksDb, "rocksDb");
            this.columnFamily = checkNotNull(columnFamily, "columnFamily");
            this.serializer = checkNotNull(serializer, "serializer");
        }

        @Override
        public V get(K key) throws IOException {
            try {
                V ret = null;
                if (key != null) {
                    byte[] rawKey = serializer.serialize(key);
                    byte[] rawData = rocksDb.get(columnFamily, rawKey);
                    ret = rawData != null ? (V) serializer.deserialize(rawData) : null;
                }
                return ret;
            } catch (RocksDBException e) {
                throw new IOException(String.format("Failed to get value by key-%s", key), e);
            }
        }

        @Override
        public Map<K, V> getBatch(Collection<K> keys) throws IOException {
            Map<K, V> batch = new HashMap<K, V>();
            for (K key : keys) {
                V value = get(key);
                if (value != null)
                    batch.put(key, value);
            }
            return batch;
        }

        @Override
        public void put(K key, V value) throws IOException {
            try {
                rocksDb.put(columnFamily, serializer.serialize(key), serializer.serialize(value));
            } catch (RocksDBException e) {
                throw new IOException(String.format("Failed to put data, key=%s, value=%s", key, value), e);
            }
        }

        @Override
        public void putBatch(Map<K, V> batch) throws IOException {
            try {
                WriteBatch writeBatch = new WriteBatch();
                for (Map.Entry<K, V> entry : batch.entrySet()) {
                    writeBatch.put(
                            columnFamily,
                            serializer.serialize(entry.getKey()),
                            serializer.serialize(entry.getValue()));
                }
                rocksDb.write(new WriteOptions(), writeBatch);
            } catch (RocksDBException e) {
                throw new IOException(String.format("Failed to put batch=%s", batch), e);
            }
        }

        @Override
        public void remove(K key) throws IOException {
            try {
                rocksDb.remove(columnFamily, serializer.serialize(key));
            } catch (RocksDBException e) {
                throw new IOException(String.format("Failed to remove key=%s", key), e);
            }
        }

        @Override
        public void removeBatch(Collection<K> keys) throws IOException {
            for (K key : keys) {
                remove(key);
            }
        }

        @Override
        public Collection<K> listKeys() {
            Collection<K> keys = new ArrayList<>();
            RocksIterator itr = rocksDb.newIterator(columnFamily);
            itr.seekToFirst();
            while (itr.isValid()) {
                keys.add((K) serializer.deserialize(itr.key()));
                itr.next();
            }
            return keys;
        }

        @Override
        public void close() {
            rocksDb.dispose();
        }
    }
}
