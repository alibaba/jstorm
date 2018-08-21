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

import com.alibaba.jstorm.metric.JStormMetrics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.state.Serializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cache.rocksdb.RocksDbOptionsFactory;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.hdfs.HdfsCache;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.transactional.state.IRichCheckpointKvState;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.SerializerFactory;

/**
 * 1. Backup checkpoint state from local FS to remote FS 
 * 2. Restore checkpoint state from remoteFS to local FS
 * 
 * HDFS state structure: base_dir/topology_name/rocksdb/key_range/0001.sst 
 *                                                                0002.sst 
 *                                                                ....... 
 *                                                                checkpoint/batchId/sstFiles.list 
 *                                                                                   CURRENT 
 *                                                                                   MANIFEST-*
 * 
 * Local RocksDB structure: worker_dir/transactionState/rocksdb/key_range/rocksdb_files 
 *                                                                        checkpoint/...
 * 
 * Transaction State
 */
public class RocksDbHdfsState<K, V> implements IRichCheckpointKvState<K, V, String> {
    private static final long serialVersionUID = 7907988526099798193L;

    private static final Logger LOG = LoggerFactory.getLogger(RocksDbHdfsState.class);

    protected static final String ROCKSDB_DATA_FILE_EXT = "sst";
    protected static final String SST_FILE_LIST = "sstFile.list";
    protected static final String ENABLE_METRICS = "rocksdb.hdfs.state.metrics";

    protected String topologyName;
    protected Map conf;

    protected String stateName;

    protected HdfsCache hdfsCache;
    protected String hdfsDbDir;
    protected String hdfsCheckpointDir;

    protected RocksDB rocksDb;
    protected String rocksDbDir;
    protected String rocksDbCheckpointDir;
    protected Collection<String> lastCheckpointFiles;

    protected Serializer<Object> serializer;

    protected int ttlTimeSec;

    protected long lastCleanTime;
    protected long cleanPeriod;

    protected long lastSuccessBatchId;

    protected MetricClient metricClient;
    protected AsmHistogram hdfsWriteLatency;
    protected AsmHistogram hdfsDeleteLatency;
    protected AsmHistogram rocksDbFlushAndCpLatency;

    public RocksDbHdfsState() {

    }

    @Override
    public void init(TopologyContext context) {
        try {
            this.topologyName = Common.topologyIdToName(context.getTopologyId());
        } catch (InvalidTopologyException e) {
            LOG.error("Failed get topology name by id-{}", context.getTopologyId());
            throw new RuntimeException(e.getMessage());
        }
        String workerDir = context.getWorkerIdDir();
        metricClient = new MetricClient(context);
        hdfsWriteLatency = metricClient.registerHistogram("HDFS write latency");
        hdfsDeleteLatency = metricClient.registerHistogram("HDFS delete latency");
        rocksDbFlushAndCpLatency = metricClient.registerHistogram("RocksDB flush and checkpoint latency");
        cleanPeriod = ConfigExtension.getTransactionBatchSnapshotTimeout(context.getStormConf()) * 5 * 1000;
        serializer = SerializerFactory.createSerailzer(context.getStormConf());
        initEnv(topologyName, context.getStormConf(), workerDir);
    }

    public void initEnv(String topologyName, Map conf, String workerDir) {
        this.conf = conf;
        // init hdfs env
        this.hdfsCache = new HdfsCache(conf);
        this.hdfsDbDir = hdfsCache.getBaseDir() + "/" + topologyName + "/rocksDb/" + stateName;
        this.hdfsCheckpointDir = hdfsDbDir + "/checkpoint";
        try {
            if (!hdfsCache.exist(hdfsDbDir))
                hdfsCache.mkdir(hdfsDbDir);
            if (!hdfsCache.exist(hdfsCheckpointDir))
                hdfsCache.mkdir(hdfsCheckpointDir);
        } catch (IOException e) {
            LOG.error("Failed to create hdfs dir for path=" + hdfsCheckpointDir, e);
            throw new RuntimeException(e.getMessage());
        }

        // init local rocksdb even
        this.rocksDbDir = workerDir + "/transactionState/rocksdb/" + stateName;
        this.rocksDbCheckpointDir = rocksDbDir + "/checkpoint";
        initLocalRocksDbDir();
        initRocksDb();

        LOG.info("HDFS: dataDir={}, checkpointDir={}", hdfsDbDir, hdfsCheckpointDir);
        LOG.info("Local: dataDir={}, checkpointDir={}", rocksDbDir, rocksDbCheckpointDir);
    } 

    protected void initRocksDb() {
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
            if (ttlTimeSec > 0)
                rocksDb = TtlDB.open(options, rocksDbDir, ttlTimeSec, false);
            else
                rocksDb = RocksDB.open(options, rocksDbDir);
            // enable compaction
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

    private void initLocalRocksDbDir() {
        try {
            File file = new File(rocksDbDir);
            if (file.exists())
                FileUtils.cleanDirectory(file);
            FileUtils.forceMkdir(new File(rocksDbCheckpointDir));
        } catch (IOException e) {
            LOG.error("Failed to create dir for path=" + rocksDbCheckpointDir, e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    @Override
    public void put(K key, V value) {
        try {
            rocksDb.put(serializer.serialize(key), serializer.serialize(value));
        } catch (RocksDBException e) {
            LOG.error("Failed to put data, key={}, value={}", key, value);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void putBatch(Map<K, V> batch) {
        try {
            WriteBatch writeBatch = new WriteBatch();
            for (Map.Entry<K, V> entry : batch.entrySet()) {
                writeBatch.put(serializer.serialize(entry.getKey()), serializer.serialize(entry.getValue()));
            }
            rocksDb.write(new WriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            LOG.error("Failed to put batch={}", batch);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public V get(K key) {
        try {
            V ret = null;
            if (key != null) {
                byte[] rawKey = serializer.serialize(key);
                byte[] rawData = rocksDb.get(rawKey);
                ret = rawData != null ? (V) serializer.deserialize(rawData) : null;
            }
            return ret;
        } catch (RocksDBException e) {
            LOG.error("Failed to get value by key-{}", key);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Map<K, V> getBatch(Collection<K> keys) {
        Map<K, V> batch = new HashMap<K, V>();
        for (K key : keys) {
            V value = get(key);
            if (value != null)
                batch.put(key, value);
        }
        return batch;
    }

    @Override
    public Map<K, V> getBatch() {
        Map<K, V> batch = new HashMap<K, V>();
        RocksIterator itr = rocksDb.newIterator();
        itr.seekToFirst();
        while (itr.isValid()) {
            byte[] rawKey = itr.key();
            byte[] rawValue = itr.value();
            V value = rawValue != null ? (V) serializer.deserialize(rawValue) : null;
            batch.put((K) serializer.deserialize(rawKey), value);
            itr.next();
        }
        return batch;
    }

    @Override
    public Collection<K> getAllKeys() {
        Collection<K> keys = new ArrayList<K>();
        RocksIterator itr = rocksDb.newIterator();
        itr.seekToFirst();
        while (itr.isValid()) {
            keys.add((K) serializer.deserialize(itr.key()));
            itr.next();
        }
        return keys;
    }

    @Override
    public void remove(K key) {
        try {
            rocksDb.remove(serializer.serialize(key));
        } catch (RocksDBException e) {
            LOG.warn("Failed to remove " + key, e);
        }
    }

    @Override
    public void clear() {
        for (K key : getAllKeys()) {
            remove(key);
        }
    }

    @Override
    public void cleanup() {
        if (rocksDb != null)
            rocksDb.dispose();
        if (hdfsCache != null)
            hdfsCache.close();
    }

    @Override
    public void restore(String checkpointBackupDir) {
        LOG.info("Start restore from remote: {}", checkpointBackupDir);

        if (rocksDb != null)
            rocksDb.dispose();
        initLocalRocksDbDir();
        // Restore db files from hdfs to local disk
        try {
            if (checkpointBackupDir != null) {
                // Get dir of sst files
                int index = checkpointBackupDir.lastIndexOf("checkpoint");
                String remoteDbBackupDir = checkpointBackupDir.substring(0, index);

                // copy sstFile.list, CURRENT, MANIFEST to local disk for the specified batch
                Collection<String> files = hdfsCache.listFile(checkpointBackupDir, false);
                LOG.debug("Restore checkpoint files: {}", files);
                for (String fileName : files)
                    hdfsCache.copyToLocal(checkpointBackupDir + "/" + fileName, rocksDbDir);

                // copy all rocksDB sst files to local disk
                String sstFileList = rocksDbDir + "/" + SST_FILE_LIST;
                File file = new File(sstFileList);
                List<String> sstFiles = FileUtils.readLines(file);
                LOG.debug("Restore sst files: {}", sstFiles);
                for (String sstFile : sstFiles) {
                    hdfsCache.copyToLocal(remoteDbBackupDir + "/" + sstFile, rocksDbDir);
                }
                FileUtils.deleteQuietly(file);
            }
            initRocksDb();
        } catch (IOException e) {
            LOG.error("Failed to restore checkpoint", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String backup(long batchId) {
        try {
            String hdfsCpDir = getRemoteCheckpointPath(batchId);
            String batchCpPath = getLocalCheckpointPath(batchId);
            long startTime = System.currentTimeMillis();

            // upload sst data files to hdfs
            Collection<File> sstFiles = FileUtils.listFiles(new File(batchCpPath), new String[] { ROCKSDB_DATA_FILE_EXT }, false);
            for (File sstFile : sstFiles) {
                if (!lastCheckpointFiles.contains(sstFile.getName())) {
                    hdfsCache.copyToDfs(batchCpPath + "/" + sstFile.getName(), hdfsDbDir, true);
                }
            }

            // upload sstFile.list, CURRENT, MANIFEST to hdfs
            Collection<String> sstFileList = getFileList(sstFiles);
            File cpFileList = new File(batchCpPath + "/" + SST_FILE_LIST);
            FileUtils.writeLines(cpFileList, sstFileList);

            if (hdfsCache.exist(hdfsCpDir))
                hdfsCache.remove(hdfsCpDir, true);

            hdfsCache.mkdir(hdfsCpDir);
            Collection<File> allFiles = FileUtils.listFiles(new File(batchCpPath), null, false);
            allFiles.removeAll(sstFiles);
            Collection<File> nonSstFiles = allFiles;
            for (File nonSstFile : nonSstFiles) {
                hdfsCache.copyToDfs(batchCpPath + "/" + nonSstFile.getName(), hdfsCpDir, true);
            }

            if (JStormMetrics.enabled)
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
     * @param batchId
     */
    @Override
    public void checkpoint(long batchId) {
        long startTime = System.currentTimeMillis();
        try {
            rocksDb.flush(new FlushOptions());
            Checkpoint cp = Checkpoint.create(rocksDb);
            cp.createCheckpoint(getLocalCheckpointPath(batchId));
        } catch (RocksDBException e) {
            LOG.error("Failed to create checkpoint for batch-" + batchId, e);
            throw new RuntimeException(e.getMessage());
        }

        if (JStormMetrics.enabled)
            rocksDbFlushAndCpLatency.update(System.currentTimeMillis() - startTime);
    }

    /**
     * remove obsolete checkpoint data at local disk and remote backup storage
     * 
     * @param batchId id of success batch
     */
    @Override
    public void remove(long batchId) {
        removeObsoleteLocalCheckpoints(batchId);
        removeObsoleteRemoteCheckpoints(batchId);
    }

    private String getLocalCheckpointPath(long batchId) {
        return rocksDbCheckpointDir + "/" + batchId;
    }

    private String getRemoteCheckpointPath(long batchId) {
        return hdfsCheckpointDir + "/" + batchId;
    }

    private String getRemoteCheckpointSstListFile(long batchId) {
        return getRemoteCheckpointPath(batchId) + "/" + SST_FILE_LIST;
    }

    private Collection<String> getFileList(Collection<File> files) {
        Collection<String> ret = new HashSet<String>();
        for (File file : files)
            ret.add(file.getName());
        return ret;
    }

    private void removeObsoleteLocalCheckpoints(long successBatchId) {
        File cpRootDir = new File(rocksDbCheckpointDir);
        for (String cpDir : cpRootDir.list()) {
            try {
                long cpId = JStormUtils.parseLong(cpDir);
                if (cpId < successBatchId)
                    FileUtils.deleteQuietly(new File(rocksDbCheckpointDir + "/" + cpDir));
            } catch (Throwable e) {
                File file = new File(rocksDbCheckpointDir + "/" + cpDir);
                // If existing more thant one hour, remove the unexpected file
                if (System.currentTimeMillis() - file.lastModified() > 60 * 60 * 1000) {
                    LOG.debug("Unexpected file-" + cpDir + " in local checkpoint dir, " + rocksDbCheckpointDir, e);
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
            Collection<String> lastSuccessSStFiles = hdfsCache.readLines(getRemoteCheckpointSstListFile(successBatchId));
            if (hdfsCache.exist(getRemoteCheckpointPath(obsoleteBatchId))) {
                // remove obsolete sst files
                Collection<String> obsoleteSstFiles = hdfsCache.readLines(getRemoteCheckpointSstListFile(obsoleteBatchId));
                obsoleteSstFiles.removeAll(lastSuccessSStFiles);
                for (String sstFile : obsoleteSstFiles) {
                    hdfsCache.remove(hdfsDbDir + "/" + sstFile, false);
                }

                // remove checkpoint dir
                hdfsCache.remove(getRemoteCheckpointPath(obsoleteBatchId), true);
            }

            // Sometimes if remove was failed, some checkpoint files would be left in remote FS.
            // So here to check if full clean is required for a specified period. If so, clean all checkpoint files which are expired.
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastCleanTime > cleanPeriod) {
                FileStatus successCpFileStatus = hdfsCache.getFileStatus(getRemoteCheckpointSstListFile(successBatchId));
                // remove obsolete sst files
                FileStatus[] fileStatuses = hdfsCache.listFileStatus(hdfsDbDir);
                for (FileStatus fileStatus : fileStatuses) {
                    String fileName = fileStatus.getPath().getName();
                    if (fileStatus.getModificationTime() < successCpFileStatus.getModificationTime() && !lastSuccessSStFiles.contains(fileName)) {
                        hdfsCache.remove(hdfsDbDir + "/" + fileName, true);
                    }
                }

                // remove obsolete checkpoint dir
                fileStatuses = hdfsCache.listFileStatus(hdfsCheckpointDir);
                for (FileStatus fileStatus : fileStatuses) {
                    String checkpointId = fileStatus.getPath().getName();
                    if (fileStatus.getModificationTime() < successCpFileStatus.getModificationTime() && Integer.valueOf(checkpointId) != successBatchId) {
                        hdfsCache.remove(hdfsCheckpointDir + "/" + checkpointId, true);
                    }
                }

                lastCleanTime = currentTime;
            }
        } catch (IOException e) {
            LOG.error("Failed to remove obsolete checkpoint data for batch-" + obsoleteBatchId, e);
        }

        if (JStormMetrics.enabled)
            this.hdfsDeleteLatency.update(System.currentTimeMillis() - startTime);
    }

    public static void main(String[] args) {
        Map conf = new HashMap<Object, Object>();
        conf.putAll(Utils.loadConf(args[0]));
        RocksDbHdfsState<String, Integer> state = new RocksDbHdfsState<String, Integer>();
        state.setStateName(String.valueOf(1));

        // setup checkpoint
        int batchNum = JStormUtils.parseInt(conf.get("batch.num"), 100);
        state.initEnv("test", conf, "/tmp/rocksdb_test");
        String remoteCpPath = null;
        for (int i = 0; i < batchNum; i++) {
            state.put(String.valueOf(i % 20), i);
            state.checkpoint(i);
            remoteCpPath = state.backup(i);
            state.remove(i);
        }
        state.cleanup();

        state.initEnv("test", conf, "/tmp/rocksdb_test");
        state.restore(remoteCpPath);
        for (int i = 0; i < 20; i++) {
            Integer value = state.get(String.valueOf(i));
            LOG.info("key={}, value={}", String.valueOf(i), value);
        }
        state.cleanup();
    }
}