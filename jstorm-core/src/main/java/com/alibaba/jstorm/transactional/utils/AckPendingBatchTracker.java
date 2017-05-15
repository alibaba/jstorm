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
package com.alibaba.jstorm.transactional.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AckPendingBatchTracker<T> {
    // Map<BatchId, Map<StreamId, BatchTrackerInfo>>
    private Map<Long, Map<String, T>> tracker;
    // Map<BatchId, StartTimestamp>
    private Map<Long, Long> batchTs;

    public AckPendingBatchTracker() {
        tracker = new ConcurrentHashMap<>();
        batchTs = new ConcurrentHashMap<>();
    }

    public Set<Long> getBatchIds() {
        return tracker.keySet();
    }

    public Map<String, T> removeBatch(long batchId) {
        batchTs.remove(batchId);
        return tracker.remove(batchId);
    }

    public Map<String, T> getPendingBatchTracker(long batchId) {
        return getPendingBatchTracker(batchId, false);
    }

    public Map<String, T> getPendingBatchTracker(long batchId, boolean create) {
        Map<String, T> ret = tracker.get(batchId);
        if (ret == null && create) {
            ret = new HashMap<>();
            tracker.put(batchId, ret);
            batchTs.put(batchId, System.currentTimeMillis());
        }
        return ret;
    }

    public Set<String> getStreamIds(long batchId) {
        Set<String> ret = new HashSet<String>();
        Map<String, T> pendingBatches = getPendingBatchTracker(batchId);
        if (pendingBatches != null) {
            ret.addAll(pendingBatches.keySet());
        }
        return ret;
    }

    public T getPendingBatch(long batchId, String streamId) {
        return getPendingBatch(batchId, streamId, false);
    }
    public T getPendingBatch(long batchId, String streamId, boolean create) {
        Map<String, T> pendingBatches = getPendingBatchTracker(batchId, create);
        T ret = pendingBatches != null ? pendingBatches.get(streamId) : null;
        return ret;
    }

    public void putPendingBatch(long batchId, String streamId, T pendingBatch) {
        Map<String, T> pendingBatches = getPendingBatchTracker(batchId, true);
        pendingBatches.put(streamId, pendingBatch);
    }

    public long getBatchStartTime(long batchId) {
        Long time = batchTs.get(batchId);
        return  time != null ? time : 0l;
    }

    public void clear() {
        tracker.clear();
    }

    @Override
    public String toString() {
        return tracker.toString();
    }
}