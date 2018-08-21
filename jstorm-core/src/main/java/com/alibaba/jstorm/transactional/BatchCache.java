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

import backtype.storm.generated.StormTopology;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.JStormUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCache {
    public static Logger LOG = LoggerFactory.getLogger(BatchCache.class);

    private TopologyContext context;
    private Map stormConf;

    protected boolean isExactlyOnceMode;
    protected ICacheOperator cacheOperator;
    protected Map<Long, PendingBatch> pendingBatches;

    protected KryoTupleSerializer serializer;
    protected KryoTupleDeserializer deserializer;

    public BatchCache(TopologyContext context, StormTopology sysTopology, boolean isIntraWorker) {
        this.context = context;
        this.stormConf = context.getStormConf();

        this.isExactlyOnceMode = JStormUtils.parseBoolean(stormConf.get("transaction.exactly.once.mode"), true);

        this.pendingBatches = new ConcurrentHashMap<>();

        serializer = new KryoTupleSerializer(stormConf, sysTopology);
        deserializer = new KryoTupleDeserializer(stormConf, context, sysTopology);

        String cacheDir = context.getWorkerIdDir() + "/transactionCache/task-" + context.getThisTaskId();
        if (isIntraWorker)
            cacheDir += "/intra";
        else
            cacheDir += "/inter";
        String cacheType = Utils.getString(stormConf.get("transaction.exactly.cache.type"), "default");
        if (cacheType.equalsIgnoreCase("rocksDb")) {
            cacheOperator = new RocksDbCacheOperator(context, cacheDir);
        } else {
            cacheOperator = new DefaultCacheOperator();
        }
        LOG.info("Cache config: isExactlyOnce={}, cacheType={}", isExactlyOnceMode, cacheType);
    }

    public boolean isExactlyOnceMode() {
        return isExactlyOnceMode;
    }

    public void cachePendingBatch(long batchId, byte[] data) {
        PendingBatch batch = pendingBatches.get(batchId);
        if (batch == null) {
            batch = cacheOperator.createPendingBatch(batchId);
            pendingBatches.put(batchId, batch);
        }

        batch.addTuples(data);
    }

    public void cachePendingBatch(long batchId, Tuple tuple) {
        cachePendingBatch(batchId, serializer.serialize(tuple));
    }

    public boolean isPendingBatch(long batchId, long lastSuccessfulBatch) {
        boolean ret = false;
        if (batchId == TransactionCommon.INIT_BATCH_ID) {
            return ret;
        }

        if (isExactlyOnceMode) {
            // If it is not the same group with current in progress batch, just put incoming tuple into pending queue
            if (batchId > lastSuccessfulBatch + 1) {
                ret = true;
            }
        }

        return ret;
    }

    public Tuple getPendingTuple(byte[] data) {
        return deserializer.deserialize(data);
    }

    public PendingBatch getNextPendingBatch(long lastSuccessfulBatch) {
        long batchId = lastSuccessfulBatch + 1;
        return pendingBatches.remove(batchId);
    }

    public void removeExpiredBatches(long lastSuccessfulBatch) {
        SortedSet<Long> pendingBatchIds = new TreeSet<>(pendingBatches.keySet());
        SortedSet<Long> expiredBatchIds = pendingBatchIds.headSet(lastSuccessfulBatch + 1);
  
        if (!expiredBatchIds.isEmpty())
            LOG.info("Following expired batches will be removed: {}", expiredBatchIds);
        for (Long expiredBatchId : expiredBatchIds) {
            PendingBatch expiredBatch = pendingBatches.remove(expiredBatchId);
            expiredBatch.removeTuples();
        } 
    }

    public void cleanup() {
        for (Entry<Long, PendingBatch> entry : pendingBatches.entrySet()) {
            PendingBatch batch = entry.getValue();
            batch.removeTuples();
        }
        pendingBatches.clear();
    }

    @Override
    public String toString() {
        return "pendingBatches: " + pendingBatches.toString();
    }
}