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
package com.alibaba.jstorm.transactional;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cache.RocksDBCache;

public class RocksDbPendingBatch extends PendingBatch {
    public static Logger LOG = LoggerFactory.getLogger(RocksDbPendingBatch.class);

    private String cacheKeyPrefix;
    private int cacheNum = 0;
    private int cacheTupleNum = 0;
    private int cacheReadIndex = 0;
    private int cacheSize = 0;

    private RocksDBCache cache;
    private int maxFlushSize;

    public RocksDbPendingBatch(RocksDbCacheOperator cache, long batchId) {
        super(batchId);
        this.cache = cache;
        this.maxFlushSize = cache.getMaxFlushSize();
        this.cacheKeyPrefix = String.valueOf(batchId);
    }

    @Override
    public void addTuples(byte[] data) {
        synchronized (lock) {
            if (!isActive)
                return;

            tuples.add(data);
            cacheSize += data.length;
            if (cacheSize > maxFlushSize) {
                cache.put(cacheKeyPrefix + cacheNum, tuples);
                cacheNum++;
                cacheTupleNum += tuples.size();
                tuples = new ArrayList<>();
                cacheSize = 0;
            }
        }
    }

    @Override
    public List<byte[]> getTuples() {
        List<byte[]> cacheBatch = null;
        synchronized (lock) {
            isActive = false;
            if (cacheReadIndex < cacheNum) {
                String key = cacheKeyPrefix + cacheReadIndex;
                cacheBatch = (List<byte[]>) cache.get(key);
                cache.remove(key);
                cacheReadIndex++;
            } else if (tuples != null && tuples.size() > 0) {
                cacheBatch = tuples;
                tuples = null;
            }
        }
        return cacheBatch;
    }

    @Override
    public void removeTuples() {
        synchronized (lock) {
            for (; cacheReadIndex < cacheNum; cacheReadIndex++) {
                String key = cacheKeyPrefix + cacheReadIndex;
                cache.remove(key);
            }
            tuples = null;
            isActive = false;
        }
    }

    @Override
    public String toString() {
        return super.toString() + ", cacheTupleNum: " + cacheTupleNum + ", cacheNum: " + cacheNum + ", cacheReadIndex: " + cacheReadIndex;
    }
}