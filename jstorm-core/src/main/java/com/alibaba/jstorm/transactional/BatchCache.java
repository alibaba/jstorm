package com.alibaba.jstorm.transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.rocksdb.Options;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.StormTopology;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.cache.RocksDBCache;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

public class BatchCache extends RocksDBCache {
    public static Logger LOG = LoggerFactory.getLogger(BatchCache.class);

    protected class PendingBatch {
        public String cacheKeyPrefix;
        public volatile int cacheNum = 0;
        public int cacheReadIndex = 0;
        public List<byte[]> tuples = new ArrayList<byte[]>();
        private int cacheSize = 0;
        private Object lock = new Object();
        private boolean isActive = true;

        public void addData(byte[] data) {
            tuples.add(data);
            cacheSize += data.length;
            if (cacheSize > maxFlushSize) {
                for (byte[] cacheData : tuples) {
                    put(cacheKeyPrefix + String.valueOf(cacheNum), cacheData);
                    cacheNum++;
                }
                tuples = new ArrayList<byte[]>();
                cacheSize = 0;
            }
        }/*
        public void addData(byte[] data) {
            tuples.add(data);
        }*/

        public boolean addTuples(byte[] data) {
            synchronized (lock) {
                if (isActive) {
                    addData(data);
                    return true;
                } else {
                    return false;
                }
            }
        }

        public boolean addTuples(KryoTupleSerializer serializer, Tuple tuple) {
            byte[] data = serializer.serialize(tuple);
            synchronized (lock) {
                if (isActive) {
                    addData(data);
                    return true;
                } else {
                    return false;
                }
            }
        }

        public List<byte[]> getTuples() {
            List<byte[]> cacheBatch = new ArrayList<byte[]>();
            synchronized (lock) {
                if (isActive) {
                    for (; cacheReadIndex < cacheNum; cacheReadIndex++) {
                        String key = cacheKeyPrefix + String.valueOf(cacheReadIndex);
                        cacheBatch.add((byte[]) get(key));
                        remove(key);
                    }
                    cacheBatch.addAll(tuples);
                    tuples = new ArrayList<byte[]>();
                    isActive = false;
                } else {
                    LOG.warn("Try to get cache tuples when cache has been read or removed!");
                }
            }
            return cacheBatch;
        }/*
        public List<byte[]> getTuples() {
            List<byte[]> ret;
            synchronized (lock) {
                if (isActive) {
                    isActive = false;
                }
                ret = tuples;
                tuples = null;
            }
            return ret;
        }*/

        public void removeTuples() {
            synchronized (lock) {
                for (; cacheReadIndex < cacheNum; cacheReadIndex++) {
                    String key = cacheKeyPrefix + String.valueOf(cacheReadIndex);
                    remove(key);
                }
                tuples = new ArrayList<byte[]>();
                isActive = false;
            }
        }/*
        public void removeTuples() {
            synchronized (lock) {
                tuples = null;
                isActive = false;
            }
        }*/

        @Override
        public String toString() {
            return "cacheNum: " + cacheNum + ", Pending tuple size:" + (tuples != null ? tuples.size() : 0);
        }
    }

    protected Map stormConf;
    protected String workerDir;
    protected String cacheDir;
    protected int taskId;

    protected boolean isExactlyOnceMode;
    protected Map<Integer, Map<Long, PendingBatch>> pendingBatches;
    protected List<Integer> pendingBatchGroups;
    protected int pendingBatchGroupIndex = 0;
    protected int maxFlushSize;

    protected KryoTupleSerializer serializer;
    protected KryoTupleDeserializer deserializer;

    public BatchCache(TopologyContext context, Set<String> upstreamSpoutIds, StormTopology sysTopology) {
        this.stormConf = context.getStormConf();
        this.workerDir = context.getWorkerIdDir();
        this.taskId = context.getThisTaskId();
        this.isExactlyOnceMode = JStormUtils.parseBoolean(stormConf.get("transaction.exactly.once.mode"), true);

        this.cacheDir = this.workerDir + "/transactionCache/task-" + taskId;

        this.pendingBatches = new HashMap<Integer, Map<Long, PendingBatch>>();
        this.pendingBatchGroups = new ArrayList<Integer>();
        for (String spoutId : upstreamSpoutIds) {
            int id = TransactionCommon.groupIndex(context.getRawTopology(), spoutId);
            pendingBatches.put(id, new HashMap<Long, PendingBatch>());
            pendingBatchGroups.add(id);
        }
        this.maxFlushSize = ConfigExtension.getTransactionCacheBatchFlushSize(stormConf);

        Options rocksDbOpt = new Options();
        rocksDbOpt.setCreateMissingColumnFamilies(true).setCreateIfMissing(true);
        long bufferSize = ConfigExtension.getTransactionCacheBlockSize(stormConf) != null ? 
                ConfigExtension.getTransactionCacheBlockSize(stormConf) : (1 * SizeUnit.GB);
        rocksDbOpt.setWriteBufferSize(bufferSize);
        int maxBufferNum = ConfigExtension.getTransactionMaxCacheBlockNum(stormConf) != null ?
                ConfigExtension.getTransactionMaxCacheBlockNum(stormConf) : 3;
        rocksDbOpt.setMaxWriteBufferNumber(maxBufferNum);

        try {
            Map<Object, Object> conf = new HashMap<Object, Object>();
            conf.put(ROCKSDB_ROOT_DIR, cacheDir);
            conf.put(ROCKSDB_RESET, true);
            initDir(conf);
            initDb(null, rocksDbOpt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        serializer = new KryoTupleSerializer(stormConf, sysTopology);
        deserializer = new KryoTupleDeserializer(stormConf, context, sysTopology);
    }

    public boolean isExactlyOnceMode() {
        return isExactlyOnceMode;
    }

    private synchronized PendingBatch getPendingBatch(BatchGroupId batchGroupId, boolean creatIfAbsent, boolean remove, Map<Integer, Long> lastSuccessfulBatch) {
        Map<Long, PendingBatch> batches = pendingBatches.get(batchGroupId.groupId);
        PendingBatch batch = batches.get(batchGroupId.batchId);
        if (batch == null && creatIfAbsent && isPendingBatch(batchGroupId, lastSuccessfulBatch)) {
            batch = new PendingBatch();
            batch.cacheKeyPrefix = String.valueOf(batchGroupId.groupId) + String.valueOf(batchGroupId.batchId);
            batches.put(batchGroupId.batchId, batch);
        } else if (batch != null) {
            if (remove) {
                batches.remove(batchGroupId.batchId);
            }
        }
        return batch;
    }

    public boolean cachePendingBatch(BatchGroupId batchGroupId, byte[] data, Map<Integer, Long> lastSuccessfulBatch) {
        PendingBatch batch = getPendingBatch(batchGroupId, true, false, lastSuccessfulBatch);
        if (batch != null) {
            return batch.addTuples(data);
        } else {
            return false;
        }
    }

    public boolean cachePendingBatch(BatchGroupId batchGroupId, Tuple tuple, Map<Integer, Long> lastSuccessfulBatch) {
        PendingBatch batch = getPendingBatch(batchGroupId, true, false, lastSuccessfulBatch);
        if (batch != null) {
            byte[] data = serializer.serialize(tuple);
            return batch.addTuples(data);
        } else {
            return false;
        }
    }

    public boolean isPendingBatch(BatchGroupId batchGroupId, Map<Integer, Long> lastSuccessfulBatch) {
        boolean ret = false;
        if (batchGroupId.batchId == TransactionCommon.INIT_BATCH_ID) {
            return ret;
        }

        if (isExactlyOnceMode) {
            // If it is not the same group with current in progress batch, just put incoming tuple into pending queue
            Long successBatchId = lastSuccessfulBatch.get(batchGroupId.groupId);
            if (batchGroupId.batchId > successBatchId + 1) {
                ret = true;
            }
        }
        
        return ret;
    }

    public List<Tuple> getNextPendingTuples(Map<Integer, Long> lastSuccessfulBatch) {
        List<Tuple> ret = null;
        List<byte[]> protoBatch = getNextPendingBatch(lastSuccessfulBatch);
        if (protoBatch != null) {
            ret = new ArrayList<Tuple>();
            for (byte[] data : protoBatch) {
                ret.add(deserializer.deserialize(data));
            }
        }
        return ret;
    }

    public List<byte[]> getNextPendingBatch(Map<Integer, Long> lastSuccessfulBatch) {
        List<byte[]> ret = null;
        PendingBatch batch = null;
        BatchGroupId batchGroupId = null;
        for (int i = 0; i < pendingBatchGroups.size(); i++) {
            int groupId = pendingBatchGroups.get(pendingBatchGroupIndex);
            pendingBatchGroupIndex = (++pendingBatchGroupIndex) % pendingBatchGroups.size();
            long batchId = lastSuccessfulBatch.get(groupId) + 1;
            batchGroupId = new BatchGroupId(groupId, batchId);
            batch = getPendingBatch(batchGroupId, false, true, lastSuccessfulBatch);
            if (batch != null) {
                // Found a non-empty pending batch, just break the loop and process it.
                ret = batch.getTuples();
                break;
            }
        }
        return ret;
    }

    public synchronized void cleanup(int groupId) {
        Map<Long, PendingBatch> batches = pendingBatches.get(groupId);
        if (batches != null) {
            for (Entry<Long, PendingBatch> entry : batches.entrySet()) {
                PendingBatch batch = entry.getValue();
                batch.removeTuples();
            }
            batches.clear();
        }
    }

    @Override
    protected byte[] serialize(Object data) {
        return (byte[]) data;
    }

    @Override 
    protected Object deserialize(byte[] data) {
        return data;
    }

    @Override
    public String toString() {
        return "pendingBatches: " + pendingBatches.toString() + ", pendingBatchGroups: " + pendingBatchGroups;
    }
}