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
package com.alibaba.jstorm;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.starter.utils.JStormHelper;

public class RocksDbUnitTest {
    private static Logger LOG = LoggerFactory.getLogger(RocksDbUnitTest.class);

    private static int putNum;
    private static boolean isFlush;
    private static boolean isCheckpoint;
    private static int sleepTime;
    private static int compactionInterval;
    private static int flushInterval;
    private static boolean isCompaction;
    private static long fileSizeBase;
    private static int levelNum;
    private static int compactionTriggerNum;
    private static String dbPath = "/tmp/rocksdb_test";
    private static String cpPath = dbPath + "/checkpoint";

    private static int getStartValue(RocksDB db, ColumnFamilyHandle handler) {
        int startValue = 0;
        SortedMap<Integer, Integer> values = new TreeMap<Integer, Integer>();
        RocksIterator itr = db.newIterator(handler);
        itr.seekToFirst();
        while(itr.isValid()) {
            Integer key = Integer.valueOf(new String(itr.key()));
            Integer value = Integer.valueOf(new String(itr.value()));
            values.put(key, value);
            itr.next();
        }
        LOG.info("Load previous db: size={}, values={}", values.size(), values);
        if (!values.isEmpty()) {
            Integer key = values.lastKey();
            startValue = values.get(key);
            startValue++;
        }
        LOG.info("Start value={}", startValue);
        return startValue;
    }

    private static void rocksDbTest(RocksDB db, List<ColumnFamilyHandle> handlers) {
        try {
            ColumnFamilyHandle handler1 = null;
            ColumnFamilyHandle handler2 = null;
            if (handlers.size() > 0) {
                // skip default column family
                handler1 = handlers.get(1);
                handler2 = handlers.get(2);
            } else {
                handler1 = db.createColumnFamily(new ColumnFamilyDescriptor("test1".getBytes()));
                handler2 = db.createColumnFamily(new ColumnFamilyDescriptor("test2".getBytes()));
            }
            int startValue1 = getStartValue(db, handler1);
            int startValue2 = getStartValue(db, handler2);;

            Checkpoint cp = Checkpoint.create(db);
       
            if (isCompaction) {
                db.compactRange();
                LOG.info("Compaction!");
            }

            long flushWaitTime = System.currentTimeMillis() + flushInterval;
            for (int i = 0; i < putNum || putNum == -1; i++) {
                db.put(handler1, String.valueOf(i % 1000).getBytes(), String.valueOf(startValue1 + i).getBytes());
                db.put(handler2, String.valueOf(i % 1000).getBytes(), String.valueOf(startValue2 + i).getBytes());
                if (isFlush && flushWaitTime <= System.currentTimeMillis()) {
                    db.flush(new FlushOptions());
                    if (isCheckpoint) {
                        cp.createCheckpoint(cpPath + "/" + i);
                    }
                    flushWaitTime = System.currentTimeMillis() + flushInterval;
                }
            }
        } catch (RocksDBException e) {
            LOG.error("Failed to put or flush", e);
        }
    }

    public static void main(String[] args) {
        Map conf = JStormHelper.LoadConf(args[0]);
        putNum = JStormUtils.parseInt(conf.get("put.number"), 100);
        isFlush = JStormUtils.parseBoolean(conf.get("is.flush"), true);
        isCheckpoint = JStormUtils.parseBoolean(conf.get("is.checkpoint"), true);
        sleepTime = JStormUtils.parseInt(conf.get("sleep.time"), 5000);
        compactionInterval = JStormUtils.parseInt(conf.get("compaction.interval"), 30000);
        flushInterval = JStormUtils.parseInt(conf.get("flush.interval"), 3000);
        isCompaction = JStormUtils.parseBoolean(conf.get("is.compaction"), true);
        fileSizeBase = JStormUtils.parseLong(conf.get("file.size.base"), 10 * SizeUnit.KB);
        levelNum = JStormUtils.parseInt(conf.get("db.level.num"), 1);
        compactionTriggerNum = JStormUtils.parseInt(conf.get("db.compaction.trigger.num"), 4);
        LOG.info("Conf={}", conf);
        
        RocksDB db;
        File file = new File(cpPath);
        file.mkdirs();

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try {
            Options options = new Options();
            options.setCreateMissingColumnFamilies(true);
            options.setCreateIfMissing(true);
            options.setTargetFileSizeBase(fileSizeBase);
            options.setMaxBackgroundFlushes(2);
            options.setMaxBackgroundCompactions(2);
            options.setCompactionStyle(CompactionStyle.LEVEL);
            options.setNumLevels(levelNum);
            options.setLevelZeroFileNumCompactionTrigger(compactionTriggerNum);

            DBOptions dbOptions = new DBOptions();
            dbOptions.setCreateMissingColumnFamilies(true);
            dbOptions.setCreateIfMissing(true);
            dbOptions.setMaxBackgroundFlushes(2);
            dbOptions.setMaxBackgroundCompactions(2);
            ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
            familyOptions.setTargetFileSizeBase(fileSizeBase);
            familyOptions.setCompactionStyle(CompactionStyle.LEVEL);
            familyOptions.setNumLevels(levelNum);
            familyOptions.setLevelZeroFileNumCompactionTrigger(compactionTriggerNum);
            List<byte[]> families = RocksDB.listColumnFamilies(options, dbPath);
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            if (families != null) {
                for (byte[] bytes : families) {
                    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(bytes, familyOptions));
                    LOG.info("Load colum family of {}", new String(bytes));
                }
            }
            
            if (columnFamilyDescriptors.size() > 0) {
                db = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
            } else {
                db = RocksDB.open(options, dbPath);
            }
        } catch (RocksDBException e) {
            LOG.error("Failed to open db", e);
            return;
        }

        rocksDbTest(db, columnFamilyHandles);
        
        db.close();
    }
}