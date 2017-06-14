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

import backtype.storm.state.Serializer;
import com.alibaba.jstorm.cache.IKvStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.rocksdb.*;
import org.slf4j.*;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class that creates and restores {@link IKvStore IKvStores} in RocksDB.
 */
public class RocksDbKvStoreManager {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksDbKvStoreManager.class);

    private final RocksDB rocksDB;
    private final ColumnFamilyOptions columnOptions;
    private final Serializer<Object> serializer;
    private final Map<String, ColumnFamilyHandle> columnFamilies;

    @VisibleForTesting
    public RocksDbKvStoreManager(RocksDB rocksDB, ColumnFamilyOptions columnOptions, Serializer<Object> serializer) {
        this.rocksDB = checkNotNull(rocksDB, "rocksDB");
        this.columnOptions = checkNotNull(columnOptions, "columnOptions");
        this.serializer = checkNotNull(serializer, "serializer");
        this.columnFamilies = Maps.newHashMap();
    }

    public <K, V> IKvStore<K, V> getOrCreate(String kvStoreId) throws IOException {
        // TODO: restore IKvStores in failures recovery.
        return RocksDbKvStoreFactory.rocksDbCache(rocksDB, getOrCreateColumnFamily(kvStoreId), serializer);
    }

    private ColumnFamilyHandle getOrCreateColumnFamily(String kvStoreId) throws IOException {
        ColumnFamilyHandle columnFamily = columnFamilies.get(kvStoreId);
        // TODO: verify existing column family is compatible with the request.
        if (columnFamily == null) {
            ColumnFamilyDescriptor columnDescriptor =
                    new ColumnFamilyDescriptor(kvStoreId.getBytes(), columnOptions);
            try {
                columnFamily = rocksDB.createColumnFamily(columnDescriptor);
                columnFamilies.put(kvStoreId, columnFamily);
            } catch (RocksDBException e) {
                throw new IOException("Error creating ColumnFamilyHandle.", e);
            }
        }
        return columnFamily;
    }

    public void close() {
        try {
            this.rocksDB.flush(new FlushOptions());
        } catch (RocksDBException e) {
            LOG.warn("Failed to flush db before cleanup", e);
        }
        this.rocksDB.dispose();
    }
}
