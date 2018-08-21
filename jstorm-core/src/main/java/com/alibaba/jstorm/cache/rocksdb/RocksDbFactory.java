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

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;
import storm.trident.operation.builtin.Max;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Factory class for {@link RocksDB}.
 */
public class RocksDbFactory {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksDbFactory.class);

    private static final String DEFAULT_COLUMN_FAMILY = "default";

    /**
     * Returns a {@link RocksDB}
     */
    public static RocksDB createDB(Map conf, String rocksDbDir) throws IOException {
        return create(conf, rocksDbDir, -1);
    }

    /**
     * Returns a {@link RocksDB} with ttl.
     */
    public static RocksDB createTtlDB(Map conf, String rocksDbDir, int ttlTimeSec) throws IOException {
        return create(conf, rocksDbDir, ttlTimeSec);
    }

    public static RocksDB create(Map conf, String rocksDbDir, int ttlTimeSec) throws IOException {
        Options options = getOptions(conf);
        try {
            RocksDB rocksDb = ttlTimeSec > 0 ? TtlDB.open(options, rocksDbDir, ttlTimeSec, false) :
                    RocksDB.open(options, rocksDbDir);
            LOG.info("Finished loading RocksDB");
            // enable compaction
            rocksDb.compactRange();
            return rocksDb;
        } catch (RocksDBException e) {
            throw new IOException("Failed to initialize RocksDb.", e);
        }
    }

    public static RocksDB createDBWithColumnFamily(Map conf, String dbPath, final Map<String, ColumnFamilyHandle> columnFamilyHandleMap) throws IOException {
        return createWithColumnFamily(conf, dbPath, columnFamilyHandleMap, -1);
    }

    public static RocksDB createTtlDBWithColumnFamily(Map conf, String dbPath, final Map<String, ColumnFamilyHandle> columnFamilyHandleMap, int ttlSec) throws IOException {
        return createWithColumnFamily(conf, dbPath, columnFamilyHandleMap, ttlSec);
    }

    public static RocksDB createWithColumnFamily(Map conf, String rocksDbDir, final Map<String, ColumnFamilyHandle> columnFamilyHandleMap, int ttlTimeSec) throws IOException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = getExistingColumnFamilyDesc(conf, rocksDbDir);
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        DBOptions dbOptions = getDBOptions(conf);

        try {
            RocksDB rocksDb = ttlTimeSec > 0 ? TtlDB.open(
                    dbOptions, rocksDbDir, columnFamilyDescriptors, columnFamilyHandles, getTtlValues(ttlTimeSec, columnFamilyDescriptors), false) :
                    RocksDB.open(dbOptions, rocksDbDir, columnFamilyDescriptors, columnFamilyHandles);
            int n = Math.min(columnFamilyDescriptors.size(), columnFamilyHandles.size());
            // skip default column
            columnFamilyHandleMap.put(DEFAULT_COLUMN_FAMILY, rocksDb.getDefaultColumnFamily());
            for (int i = 1; i < n; i++) {
                ColumnFamilyDescriptor descriptor = columnFamilyDescriptors.get(i);
                columnFamilyHandleMap.put(new String(descriptor.columnFamilyName()), columnFamilyHandles.get(i));
            }
            LOG.info("Finished loading RocksDB with existing column family={}, dbPath={}, ttlSec={}",
                    columnFamilyHandleMap.keySet(), rocksDbDir, ttlTimeSec);
            // enable compaction
            rocksDb.compactRange();
            return rocksDb;
        } catch (RocksDBException e) {
            throw new IOException("Failed to initialize RocksDb.", e);
        }
    }

    private static List<Integer> getTtlValues(int ttlSec, List<ColumnFamilyDescriptor> descriptors) {
        List<Integer> ttlValues = Lists.newArrayList();
        for (ColumnFamilyDescriptor descriptor : descriptors) {
            ttlValues.add(ttlSec);
        }
        return ttlValues;
    }

    private static List<ColumnFamilyDescriptor> getExistingColumnFamilyDesc(Map conf, String dbPath) throws IOException {
        try {
            List<byte[]> families = Lists.newArrayList();
            List<byte[]> existingFamilies = RocksDB.listColumnFamilies(getOptions(conf), dbPath);
            if (existingFamilies != null) {
                families.addAll(existingFamilies);
            } else {
                families.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            }

            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            for (byte[] bytes : families) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(bytes, getColumnFamilyOptions(conf)));
                LOG.info("Load column family of {}", new String(bytes));
            }
            return columnFamilyDescriptors;
        } catch (RocksDBException e) {
            throw new IOException("Failed to retrieve existing column families.", e);
        }
    }

    private RocksDbFactory() {}

    public static Options getOptions(Map conf) {
        Options options = (new RocksDbOptionsFactory.Defaults()).createOptions(null);
        String optionsFactoryClass = (String) conf.get(ConfigExtension.ROCKSDB_OPTIONS_FACTORY_CLASS);
        if (optionsFactoryClass != null) {
            RocksDbOptionsFactory udfOptionFactory = (RocksDbOptionsFactory) Utils.newInstance(optionsFactoryClass);
            options = udfOptionFactory.createOptions(options);
        }
        return options;
    }

    public static DBOptions getDBOptions(Map conf) {
        DBOptions dbOptions = (new RocksDbOptionsFactory.Defaults()).createDbOptions(null);
        String optionsFactoryClass = (String) conf.get(ConfigExtension.ROCKSDB_OPTIONS_FACTORY_CLASS);
        if (optionsFactoryClass != null) {
            RocksDbOptionsFactory udfOptionFactory = (RocksDbOptionsFactory) Utils.newInstance(optionsFactoryClass);
            dbOptions = udfOptionFactory.createDbOptions(dbOptions);
        }
        return dbOptions;
    }

    public static ColumnFamilyOptions getColumnFamilyOptions(Map conf) {
        ColumnFamilyOptions cfOptions = (new RocksDbOptionsFactory.Defaults()).createColumnFamilyOptions(null);
        String optionsFactoryClass = (String) conf.get(ConfigExtension.ROCKSDB_OPTIONS_FACTORY_CLASS);
        if (optionsFactoryClass != null) {
            RocksDbOptionsFactory udfOptionFactory = (RocksDbOptionsFactory) Utils.newInstance(optionsFactoryClass);
            cfOptions = udfOptionFactory.createColumnFamilyOptions(cfOptions);
        }
        return cfOptions;
    }

    public static void cleanRocksDbLocalDir(String rocksDbDir) throws IOException {
        File file = new File(rocksDbDir);
        if (file.exists()) {
            FileUtils.cleanDirectory(file);
        } else {
            FileUtils.forceMkdir(file);
        }
    }
}
