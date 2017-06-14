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
package com.alibaba.jstorm.hdfs.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cache.rocksdb.RocksDbOptionsFactory;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.window.IKvWindowedState;
import com.alibaba.jstorm.window.IRichCheckpointWindowedState;
import com.alibaba.jstorm.window.TimeWindow;

public class WindowedRocksDbHdfsState<K, V> extends RocksDbHdfsState<K, V> implements IRichCheckpointWindowedState<K, V, String> {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedRocksDbHdfsState.class);

    private Map<TimeWindow, ColumnFamilyHandle> windowToCFHandler;

    @Override
    protected void initRocksDb() {
        windowToCFHandler = new HashMap<>();

        RocksDbOptionsFactory optionFactory = new RocksDbOptionsFactory.Defaults();
        Options options = optionFactory.createOptions(null);
        DBOptions dbOptions = optionFactory.createDbOptions(null);
        ColumnFamilyOptions cfOptions = optionFactory.createColumnFamilyOptions(null);
        String optionsFactoryClass = (String) conf.get(ConfigExtension.ROCKSDB_OPTIONS_FACTORY_CLASS);
        if (optionsFactoryClass != null) {
            RocksDbOptionsFactory udfOptionFactory = (RocksDbOptionsFactory) Utils.newInstance(optionsFactoryClass);
            options = udfOptionFactory.createOptions(options);
            dbOptions = udfOptionFactory.createDbOptions(dbOptions);
            cfOptions = udfOptionFactory.createColumnFamilyOptions(cfOptions);
        }

        try {
            ttlTimeSec = ConfigExtension.getStateTtlTime(conf);
            List<Integer> ttlValues = new ArrayList<>();

            List<byte[]> families = RocksDB.listColumnFamilies(options, rocksDbDir);
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            if (families != null) {
                for (byte[] bytes : families) {
                    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(bytes, cfOptions));
                    LOG.debug("Load colum family of {}", new String(bytes));
                    if (ttlTimeSec > 0)
                        ttlValues.add(ttlTimeSec);
                }
            }
            
            if (columnFamilyDescriptors.size() > 0) {
                if (ttlTimeSec > 0)
                    rocksDb = TtlDB.open(dbOptions, rocksDbDir, columnFamilyDescriptors, columnFamilyHandles, ttlValues, false);
                else
                    rocksDb = RocksDB.open(dbOptions, rocksDbDir, columnFamilyDescriptors, columnFamilyHandles);

                int n = Math.min(columnFamilyDescriptors.size(), columnFamilyHandles.size());
                LOG.info("Try to load RocksDB with column family, desc_num={}, handler_num={}", columnFamilyDescriptors.size(), columnFamilyHandles.size());
                // skip default column
                for (int i = 1; i < n; i++) {
                    windowToCFHandler.put((TimeWindow) serializer.deserialize(columnFamilyDescriptors.get(i).columnFamilyName()), columnFamilyHandles.get(i));
                }
            } else {
                rocksDb = RocksDB.open(options, rocksDbDir);
            }
            rocksDb.compactRange();
            LOG.info("Finish the initialization of RocksDB");
        } catch (RocksDBException e) {
            LOG.error("Failed to open rocksdb located at " + rocksDbDir, e);
            throw new RuntimeException(e.getMessage());
        }

        lastCheckpointFiles = new HashSet<String>();
        lastCleanTime = System.currentTimeMillis();
        lastSuccessBatchId = -1;
    }

    private ColumnFamilyHandle getColumnFamilyHandle(TimeWindow window) throws RocksDBException {
        ColumnFamilyHandle handler = null;
        if (window == null) {
            handler = rocksDb.getDefaultColumnFamily();
        } else {
            handler = windowToCFHandler.get(window);
            if (handler == null) {
                handler = rocksDb.createColumnFamily(new ColumnFamilyDescriptor(serializer.serialize(window)));
                windowToCFHandler.put(window, handler);
            }
        }
        return handler;
    }

    @Override
    public void put(K key, V value) {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public void putBatch(Map<K, V> batch) {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public V get(K key) {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public Map<K, V> getBatch(Collection<K> keys) {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public Map<K, V> getBatch() {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public Collection<K> getAllKeys() {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public void remove(K key) {
        throw new RuntimeException("This method shall NOT be called in windowed state instance.");
    }

    @Override
    public V get(TimeWindow window, K key) {
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            V ret = null;
            if (key != null) {
                byte[] rawKey = serializer.serialize(key);
                byte[] rawData = rocksDb.get(handler, rawKey);
                ret = rawData != null ? (V) serializer.deserialize(rawData) : null;
            }
            return ret;
        } catch (RocksDBException e) {
            LOG.error("Failed to get value by key-{} for timeWindow={}", key, window);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void put(TimeWindow window, Object key, Object value) {
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            rocksDb.put(handler, serializer.serialize(key), serializer.serialize(value));
        } catch (RocksDBException e) {
            LOG.error("Failed to put data, key={}, value={}, for timeWindow={}", key, value, window);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void putBatch(TimeWindow window, Map<K, V> batch) {
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            WriteBatch writeBatch = new WriteBatch();
            for (Map.Entry<K, V> entry : batch.entrySet()) {
                writeBatch.put(handler, serializer.serialize(entry.getKey()), serializer.serialize(entry.getValue()));
            }
            rocksDb.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            LOG.error("Failed to put batch={} for window={}", batch, window);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Map<K, V> getBatch(TimeWindow window, Collection<K> keys) {
        Map<K, V> batch = new HashMap<K, V>();
        for (K key : keys) {
            V value = get(window, key);
            if (value != null)
                batch.put(key, value);
        }
        return batch;
    }

    @Override
    public Collection<K> getAllKeys(TimeWindow window) {
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            Collection<K> keys = new ArrayList<K>();
            RocksIterator itr = rocksDb.newIterator(handler);
            itr.seekToFirst();
            while (itr.isValid()) {
                keys.add((K) serializer.deserialize(itr.key()));
                itr.next();
            }
            return keys;
        } catch (RocksDBException e) {
            LOG.error("Failed to get all keys for timeWindow={}", window);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Map<K, V> getBatch(TimeWindow window) {
        Map<K, V> batch = new HashMap<K, V>();
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            RocksIterator itr = rocksDb.newIterator(handler);
            itr.seekToFirst();
            while (itr.isValid()) {
                byte[] rawKey = itr.key();
                byte[] rawValue = itr.value();
                V value = rawValue != null ? (V) serializer.deserialize(rawValue) : null;
                batch.put((K) serializer.deserialize(rawKey), value);
                itr.next();
            }
            return batch;
        } catch (RocksDBException e) {
            LOG.error("Failed to get batch for window={}", window);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void remove(TimeWindow window, Object key) {
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            rocksDb.remove(handler, serializer.serialize(key));
        } catch (RocksDBException e) {
            LOG.warn("Failed to remove " + key + "for timeWindow=" + window, e);
        }
    }

    @Override
    public Collection<TimeWindow> listWindows() {
        return windowToCFHandler.keySet();
    }

    @Override
    public void removeWindow(TimeWindow window) {
        try {
            ColumnFamilyHandle handler = getColumnFamilyHandle(window);
            rocksDb.dropColumnFamily(handler);
            windowToCFHandler.remove(window);
        } catch (Exception e) {
            LOG.error("Failed to remove timeWindow={}", window);
        }
    }

    @Override
    public void clear() {
        Iterator<Entry<TimeWindow, ColumnFamilyHandle>> itr = windowToCFHandler.entrySet().iterator();
        while (itr.hasNext()) {
            TimeWindow window = itr.next().getKey();
            ColumnFamilyHandle handler = itr.next().getValue();
            try {
                rocksDb.dropColumnFamily(handler);
            } catch (Exception e) {
                LOG.error("Failed to remove timeWindow={}", window);
            }
        }
        windowToCFHandler.clear();
    }
}