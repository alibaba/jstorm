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

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

public interface RocksDbOptionsFactory {
    public Options createOptions(Options currentOptions);

    public DBOptions createDbOptions(DBOptions currentOptions);

    public ColumnFamilyOptions createColumnFamilyOptions(ColumnFamilyOptions currentOptions);

    public class Defaults implements RocksDbOptionsFactory {
        private static final int DEFAULT_BLOOM_FILTER_BITS = 10;

        @Override
        public Options createOptions(Options currentOptions) {
            if (currentOptions == null)
                currentOptions = new Options();
            currentOptions.setCreateIfMissing(true);
            currentOptions.setCreateMissingColumnFamilies(true);
            currentOptions.setMergeOperator(new StringAppendOperator());

            BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
            tableOptions.setBlockSize(32 * SizeUnit.KB);

            // Set memory table size
            currentOptions.setMaxWriteBufferNumber(4);
            currentOptions.setWriteBufferSize(64 * SizeUnit.MB);

            // Set block cache size
            tableOptions.setBlockCacheSize(64 * SizeUnit.MB);
            tableOptions.setFilter(new BloomFilter(DEFAULT_BLOOM_FILTER_BITS, false));
            // Put all index into block cache
            tableOptions.setCacheIndexAndFilterBlocks(true);

            /*
            tableOptions.setIndexType(IndexType.kHashSearch);
            tableOptions.setWholeKeyFiltering(false);
            currentOptions.setMemTableConfig(new HashLinkedListMemTableConfig());
            currentOptions.useFixedLengthPrefixExtractor(Integer.SIZE / Byte.SIZE * 2);
            currentOptions.setMemtablePrefixBloomBits(10000000);
            currentOptions.setMemtablePrefixBloomProbes(6);
            */

            currentOptions.setTableFormatConfig(tableOptions);

            //currentOptions.setStatsDumpPeriodSec(300);
            //currentOptions.createStatistics();

            currentOptions.setTargetFileSizeBase(64 * SizeUnit.MB);
            currentOptions.setAllowOsBuffer(true);
            currentOptions.setMaxOpenFiles(-1);
            currentOptions.setMaxBackgroundFlushes(2);
            currentOptions.setMaxBackgroundCompactions(2);
            currentOptions.setCompactionStyle(CompactionStyle.LEVEL);
            currentOptions.setLevelZeroFileNumCompactionTrigger(4);
            currentOptions.setLevelZeroSlowdownWritesTrigger(20);
            currentOptions.setLevelZeroStopWritesTrigger(30);
            currentOptions.setNumLevels(4);
            currentOptions.setMaxBytesForLevelBase(64 * 4 * SizeUnit.MB);
            currentOptions.setAllowOsBuffer(false);
            return currentOptions;
        }

        @Override
        public DBOptions createDbOptions(DBOptions currentOptions) {
            if (currentOptions == null)
                currentOptions = new DBOptions();
            currentOptions.setCreateIfMissing(true);
            currentOptions.setCreateMissingColumnFamilies(true);
            //currentOptions.setMemTableConfig(new HashLinkedListMemTableConfig());
            //currentOptions.setStatsDumpPeriodSec(300);
            //currentOptions.createStatistics();
            currentOptions.setAllowOsBuffer(true);
            currentOptions.setMaxOpenFiles(-1);
            currentOptions.setMaxBackgroundFlushes(2);
            currentOptions.setMaxBackgroundCompactions(2);
            currentOptions.setAllowOsBuffer(false);
            return currentOptions;
        }

        @Override
        public ColumnFamilyOptions createColumnFamilyOptions(ColumnFamilyOptions currentOptions) {
            if (currentOptions == null)
                currentOptions = new ColumnFamilyOptions();
            currentOptions.setMergeOperator(new StringAppendOperator());

            BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
            tableOptions.setBlockSize(32 * SizeUnit.KB);

            // Set memory table size
            currentOptions.setMaxWriteBufferNumber(4);
            currentOptions.setWriteBufferSize(64 * SizeUnit.MB);

            // Set block cache size
            tableOptions.setBlockCacheSize(64 * SizeUnit.MB);
            tableOptions.setFilter(new BloomFilter(DEFAULT_BLOOM_FILTER_BITS, false));
            // Put all index into block cache
            tableOptions.setCacheIndexAndFilterBlocks(true);
            /*
            tableOptions.setIndexType(IndexType.kHashSearch);
            tableOptions.setWholeKeyFiltering(false);*/

            /*
            currentOptions.useFixedLengthPrefixExtractor(Integer.SIZE / Byte.SIZE * 2);
            currentOptions.setMemtablePrefixBloomBits(10000000);
            currentOptions.setMemtablePrefixBloomProbes(6);
            */

            currentOptions.setTableFormatConfig(tableOptions);

            currentOptions.setTargetFileSizeBase(64 * SizeUnit.MB);
            currentOptions.setCompactionStyle(CompactionStyle.LEVEL);
            currentOptions.setLevelZeroFileNumCompactionTrigger(4);
            currentOptions.setLevelZeroSlowdownWritesTrigger(20);
            currentOptions.setLevelZeroStopWritesTrigger(30);
            currentOptions.setNumLevels(4);
            currentOptions.setMaxBytesForLevelBase(64 * 4 * SizeUnit.MB);
            return currentOptions;
        }
    }
}