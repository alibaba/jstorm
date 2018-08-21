/*
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

import backtype.storm.Config;
import backtype.storm.state.Serializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cache.IKvStore;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.dfs.DfsFactory;
import com.alibaba.jstorm.dfs.IDfs;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;
import org.slf4j.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class that creates and restores {@link IKvStore IKvStores} in RocksDB.
 */
public class RocksDbKvStoreManagerFactory {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksDbKvStoreManagerFactory.class);

    private static final int INVALID_TTL_TIME = -1;

    private static Map<String, IKvStoreManager> rocksDbKvStoreManagers = Maps.newHashMap();

    public static <T> IKvStoreManager<T> getManager(Map conf, String storeName, String dbPath, Serializer<Object> serializer) throws IOException {
        return createManager(conf, storeName, dbPath, serializer, false, INVALID_TTL_TIME);
    }

    public static <T> IKvStoreManager<T> getManager(Map conf, String storeName, String dbPath, Serializer<Object> serializer, int ttlSec) throws IOException {
        return createManager(conf, storeName, dbPath, serializer, false, ttlSec);
    }

    public static <T> IKvStoreManager<T> getStatefulManager(Map conf, String storeName, String dbPath, Serializer<Object> serializer) throws IOException {
        return createManager(conf, storeName, dbPath, serializer, true, INVALID_TTL_TIME);
    }

    public static <T> IKvStoreManager<T> getStatefulManager(Map conf, String storeName, String dbPath, Serializer<Object> serializer, int ttlSec) throws IOException {
        return createManager(conf, storeName, dbPath, serializer, true, ttlSec);
    }

    public synchronized static <T> IKvStoreManager<T> createManager(Map conf, String storeName, String dbPath, Serializer<Object> serializer, boolean isStateful, int ttlSec) throws IOException {
        IKvStoreManager kvStoreManager = rocksDbKvStoreManagers.get(dbPath);
        if (kvStoreManager == null) {
            if (isStateful) {
                kvStoreManager = new StatefulManager(conf, storeName, dbPath, serializer, ttlSec);
            } else {
                kvStoreManager = new Manager(conf, storeName, dbPath, serializer, ttlSec);
            }
            rocksDbKvStoreManagers.put(dbPath, kvStoreManager);
        }
        return kvStoreManager;
    }

    private static class Manager implements IKvStoreManager<String> {
        protected final Map conf;
        protected final String storeName;
        protected final String rocksDbPath;
        protected final String rocksDbCheckpointPath;
        protected RocksDB rocksDB;
        protected final int ttlSec;
        protected final ColumnFamilyOptions columnOptions;
        protected final Serializer<Object> serializer;
        protected final Map<String, ColumnFamilyHandle> columnFamilies;

        @VisibleForTesting
        public Manager(Map conf, String storeName, String dbPath, Serializer<Object> serializer, int ttlSec) throws IOException {
            this.conf = conf;
            this.storeName = storeName;
            this.rocksDbPath = dbPath;
            this.rocksDbCheckpointPath = rocksDbPath + "/checkpoint";
            this.ttlSec = ttlSec;
            this.serializer = checkNotNull(serializer, "serializer");

            this.columnOptions = RocksDbFactory.getColumnFamilyOptions(conf);
            this.columnFamilies = Maps.newHashMap();

            RocksDbFactory.cleanRocksDbLocalDir(rocksDbPath);
            RocksDbFactory.cleanRocksDbLocalDir(rocksDbCheckpointPath);
            initRocksDbEnv(conf, dbPath, ttlSec);
        }

        protected void initRocksDbEnv(Map conf, String dbPath, int ttlSec) throws IOException {
            columnFamilies.clear();
            this.rocksDB = ttlSec > 0 ? RocksDbFactory.createDBWithColumnFamily(conf, dbPath, columnFamilies) :
                    RocksDbFactory.createTtlDBWithColumnFamily(conf, dbPath, columnFamilies, ttlSec);
            LOG.info("Finished init RocksDb: dataDir={}, checkpointDir={}", rocksDbPath, rocksDbCheckpointPath);
        }

        @Override
        public <K, V> IKvStore<K, V> getOrCreate(String kvStoreId) throws IOException {
            return RocksDbKvStoreFactory.rocksDbCache(rocksDB, getOrCreateColumnFamily(kvStoreId), serializer);
        }

        private synchronized ColumnFamilyHandle getOrCreateColumnFamily(String kvStoreId) throws IOException {
            ColumnFamilyHandle columnFamily = columnFamilies.get(kvStoreId);
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

        @Override
        public void close() {
            try {
                this.rocksDB.flush(new FlushOptions());
            } catch (RocksDBException e) {
                LOG.warn("Failed to flush db before cleanup", e);
            }
            this.rocksDB.dispose();
        }

        public void initMonitor(TopologyContext context) {
            // Do nothing
        }

        @Override
        public void restore(String checkpoint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String backup(long checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(long checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(long checkpointId) {
            throw new UnsupportedOperationException();
        }
    }

    private static class StatefulManager extends Manager {
        private static final String ROCKSDB_DATA_FILE_EXT = "sst";
        private static final String SST_FILE_LIST = "sstFile.list";
        private static final int ROCKSDB_EXPIRED_CHECKPOINT_CLEAN_TIME = 60 * 60 * 1000; // ms

        private IDfs dfs;
        private String dfsDbPath;
        private String dfsCheckpointPath;

        private Collection<String> lastCheckpointFiles;
        private long lastCleanTime;
        private long cleanPeriod;

        private long lastSuccessBatchId;

        protected boolean isEnableMetrics = false;
        protected MetricClient metricClient;
        protected AsmHistogram rocksDbFlushAndCpLatency;
        private AsmHistogram hdfsWriteLatency;
        private AsmHistogram hdfsDeleteLatency;

        public StatefulManager(Map conf, String storeName, String dbPath, Serializer<Object> serializer, int ttlSec) throws IOException {
            super(conf, storeName, dbPath, serializer, ttlSec);
            initEnv();
            initDfsEnv(conf);
        }

        private void initEnv() {
            lastCheckpointFiles = new HashSet<String>();
            lastCleanTime = System.currentTimeMillis();
            lastSuccessBatchId = -1;
        }

        private void initDfsEnv(Map conf) throws IOException {
            dfs = DfsFactory.getHdfsInstance(conf);
            String topologyName = Utils.getString(conf.get(Config.TOPOLOGY_NAME));
            dfsDbPath = String.format("%s/%s/rocksdb", dfs.getBaseDir(), topologyName + "/rocksDb/" + storeName);;
            dfsCheckpointPath = dfsDbPath + "/checkpoint";
            if (!dfs.exist(dfsDbPath))
                dfs.mkdir(dfsDbPath);
            if (!dfs.exist(dfsCheckpointPath))
                dfs.mkdir(dfsCheckpointPath);
            LOG.info("Finished init HDFS: dataDir={}, checkpointDir={}", dfsDbPath, dfsCheckpointPath);
        }

        @Override
        public void restore(String checkpoint) {
            LOG.info("Start restore checkpoint-{} from remote: {}", checkpoint, dfsCheckpointPath);

            if (rocksDB != null)
                rocksDB.dispose();

            // Restore db files from hdfs to local disk
            try {
                RocksDbFactory.cleanRocksDbLocalDir(rocksDbPath);
                RocksDbFactory.cleanRocksDbLocalDir(rocksDbCheckpointPath);
                if (checkpoint != null) {
                    if (dfsCheckpointPath != null) {
                        // Get dir of sst files
                        int index = dfsCheckpointPath.lastIndexOf("checkpoint");
                        String remoteDbBackupDir = dfsCheckpointPath.substring(0, index);

                        // copy sstFile.list, CURRENT, MANIFEST to local disk for the specified batch
                        Collection<String> files = dfs.listFile(dfsCheckpointPath, false);
                        LOG.debug("Restore checkpoint files: {}", files);
                        for (String fileName : files)
                            dfs.copyToLocal(dfsCheckpointPath + "/" + fileName, rocksDbPath);

                        // copy all rocksDB sst files to local disk
                        String sstFileList = rocksDbPath + "/" + SST_FILE_LIST;
                        File file = new File(sstFileList);
                        List<String> sstFiles = FileUtils.readLines(file);
                        LOG.debug("Restore sst files: {}", sstFiles);
                        for (String sstFile : sstFiles) {
                            dfs.copyToLocal(remoteDbBackupDir + "/" + sstFile, rocksDbPath);
                        }
                        FileUtils.deleteQuietly(file);
                    }
                }
                initEnv();
                initRocksDbEnv(conf, rocksDbPath, ttlSec);
            } catch (IOException e) {
                LOG.error("Failed to restore checkpoint", e);
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public String backup(long checkpointId) {
            try {
                String hdfsCpDir = getDfsCheckpointPath(checkpointId);
                String batchCpPath = getRocksDbCheckpointPath(checkpointId);
                long startTime = System.currentTimeMillis();

                // upload sst data files to hdfs
                Collection<File> sstFiles = FileUtils.listFiles(new File(batchCpPath), new String[] { ROCKSDB_DATA_FILE_EXT }, false);
                for (File sstFile : sstFiles) {
                    if (!lastCheckpointFiles.contains(sstFile.getName())) {
                        dfs.copyToDfs(batchCpPath + "/" + sstFile.getName(), dfsDbPath, true);
                    }
                }

                // upload sstFile.list, CURRENT, MANIFEST to hdfs
                Collection<String> sstFileList = getFileList(sstFiles);
                File cpFileList = new File(batchCpPath + "/" + SST_FILE_LIST);
                FileUtils.writeLines(cpFileList, sstFileList);

                if (dfs.exist(hdfsCpDir))
                    dfs.remove(hdfsCpDir, true);

                dfs.mkdir(hdfsCpDir);
                Collection<File> allFiles = FileUtils.listFiles(new File(batchCpPath), null, false);
                allFiles.removeAll(sstFiles);
                Collection<File> nonSstFiles = allFiles;
                for (File nonSstFile : nonSstFiles) {
                    dfs.copyToDfs(batchCpPath + "/" + nonSstFile.getName(), hdfsCpDir, true);
                }

                if (isEnableMetrics && JStormMetrics.enabled)
                    hdfsWriteLatency.update(System.currentTimeMillis() - startTime);
                lastCheckpointFiles = sstFileList;
                return hdfsCpDir;
            } catch (IOException e) {
                LOG.error("Failed to upload checkpoint", e);
                throw new RuntimeException(e.getMessage());
            }
        }

        /**
         * Flush the data in memtable of RocksDB into disk, and then create checkpoint
         *
         * @param checkpointId
         */
        @Override
        public void checkpoint(long checkpointId) {
            long startTime = System.currentTimeMillis();
            try {
                rocksDB.flush(new FlushOptions());
                Checkpoint cp = Checkpoint.create(rocksDB);
                cp.createCheckpoint(getRocksDbCheckpointPath(checkpointId));
            } catch (RocksDBException e) {
                LOG.error(String.format("Failed to create checkpoint for checkpointId-%d", checkpointId), e);
                throw new RuntimeException(e.getMessage());
            }

            if (isEnableMetrics && JStormMetrics.enabled)
                rocksDbFlushAndCpLatency.update(System.currentTimeMillis() - startTime);
        }

        @Override
        public void remove(long checkpointId) {
            removeObsoleteRocksDbCheckpoints(checkpointId);
            removeObsoleteRemoteCheckpoints(checkpointId);
        }

        public void initMonitor(TopologyContext context) {
            isEnableMetrics = true;
            metricClient = new MetricClient(context);
            rocksDbFlushAndCpLatency = metricClient.registerHistogram("RocksDB flush and checkpoint latency");
            hdfsWriteLatency = metricClient.registerHistogram("HDFS write latency");
            hdfsDeleteLatency = metricClient.registerHistogram("HDFS delete latency");
        }

        private String getRocksDbCheckpointPath(long checkpointId) {
            return rocksDbCheckpointPath + "/" + checkpointId;
        }

        private String getDfsCheckpointPath(long checkpointId) {
            return dfsCheckpointPath + "/" + checkpointId;
        }

        private String getDfsCheckpointSstListFile(long checkpointId) {
            return getDfsCheckpointPath(checkpointId) + "/" + SST_FILE_LIST;
        }

        private Collection<String> getFileList(Collection<File> files) {
            Collection<String> ret = new HashSet<String>();
            for (File file : files)
                ret.add(file.getName());
            return ret;
        }

        private void removeObsoleteRocksDbCheckpoints(long successCheckpointId) {
            File cpRootDir = new File(rocksDbCheckpointPath);
            for (String cpDir : cpRootDir.list()) {
                try {
                    long cpId = JStormUtils.parseLong(cpDir);
                    if (cpId < successCheckpointId)
                        FileUtils.deleteQuietly(new File(rocksDbCheckpointPath + "/" + cpDir));
                } catch (Throwable e) {
                    File file = new File(rocksDbCheckpointPath + "/" + cpDir);
                    // If existing more thant one hour, remove the unexpected file
                    if (System.currentTimeMillis() - file.lastModified() > ROCKSDB_EXPIRED_CHECKPOINT_CLEAN_TIME) {
                        LOG.debug("Unexpected file-" + cpDir + " in local checkpoint dir, " + rocksDbCheckpointPath, e);
                        FileUtils.deleteQuietly(file);
                    }
                }
            }
        }

        private void removeObsoleteRemoteCheckpoints(long successBatchId) {
            long startTime = System.currentTimeMillis();
            if (lastSuccessBatchId != -1 && lastSuccessBatchId != (successBatchId - 1)) {
                LOG.warn("Some ack msgs from TM were lost!. Last success batch Id: {}, Current success batch Id: {}", lastSuccessBatchId, successBatchId);
            }
            lastSuccessBatchId = successBatchId;
            long obsoleteBatchId = successBatchId - 1;
            try {
                Collection<String> lastSuccessSStFiles = dfs.readLines(getDfsCheckpointSstListFile(successBatchId));
                if (dfs.exist(getDfsCheckpointPath(obsoleteBatchId))) {
                    // remove obsolete sst files
                    Collection<String> obsoleteSstFiles = dfs.readLines(getDfsCheckpointSstListFile(obsoleteBatchId));
                    obsoleteSstFiles.removeAll(lastSuccessSStFiles);
                    for (String sstFile : obsoleteSstFiles) {
                        dfs.remove(dfsDbPath + "/" + sstFile, false);
                    }

                    // remove checkpoint dir
                    dfs.remove(getDfsCheckpointPath(obsoleteBatchId), true);
                }
            } catch (IOException e) {
                LOG.error("Failed to remove obsolete checkpoint data for batch-" + obsoleteBatchId, e);
            }

            if (isEnableMetrics && JStormMetrics.enabled)
                this.hdfsDeleteLatency.update(System.currentTimeMillis() - startTime);
        }

        @Override
        public void close() {
            super.close();
            dfs.close();
        }
    }
}
