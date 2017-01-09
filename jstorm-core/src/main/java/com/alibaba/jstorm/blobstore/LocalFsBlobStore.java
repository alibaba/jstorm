/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.blobstore;

import backtype.storm.Config;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.nimbus.NimbusInfo;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


/**
 * Provides a local file system backed blob store implementation for Nimbus.
 *
 * For a local blob store the user and the supervisor use NimbusBlobStore Client API in order to talk to nimbus through thrift.
 * The authentication and authorization here is based on the subject.
 * We currently have NIMBUS_ADMINS and SUPERVISOR_ADMINS configuration. NIMBUS_ADMINS are given READ, WRITE and ADMIN
 * access whereas the SUPERVISOR_ADMINS are given READ access in order to read and download the blobs form the nimbus.
 *
 * The ACLs for the blob store are validated against whether the subject is a NIMBUS_ADMIN, SUPERVISOR_ADMIN or USER
 * who has read, write or admin privileges in order to perform respective operations on the blob.
 *
 * For local blob store
 * 1. The USER interacts with nimbus to upload and access blobs through NimbusBlobStore Client API.
 * 2. The USER sets the ACLs, and the blob access is validated against these ACLs.
 * 3. The SUPERVISOR interacts with nimbus through the NimbusBlobStore Client API to download the blobs.
 * The supervisors principal should match the set of users configured into SUPERVISOR_ADMINS.
 * Here, the PrincipalToLocalPlugin takes care of mapping the principal to user name before the ACL validation.
 */
public class LocalFsBlobStore extends BlobStore {
    public static final Logger LOG = LoggerFactory.getLogger(LocalFsBlobStore.class);
    private static final String DATA_PREFIX = "data_";
    private static final String META_PREFIX = "meta_";
    private final String BLOBSTORE_SUBTREE = "/blobstore/";
    private NimbusInfo nimbusInfo;
    private FileBlobStoreImpl fbs;
    private Map conf;
    private CuratorFramework zkClient;
    private final ConcurrentMap<String, ReentrantLock> topologyLock = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map conf, String overrideBase, NimbusInfo nimbusInfo) {
        this.conf = conf;
        this.nimbusInfo = nimbusInfo;

        if (overrideBase == null) {
            overrideBase = (String) conf.get(Config.BLOBSTORE_DIR);
            if (overrideBase == null) {
                overrideBase = (String) conf.get(Config.STORM_LOCAL_DIR);
            }
        }
        File baseDir = new File(overrideBase, BASE_BLOBS_DIR_NAME);
        try {
            zkClient = BlobStoreUtils.createZKClient(conf);
            fbs = new FileBlobStoreImpl(baseDir, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AtomicOutputStream createBlob(String key, SettableBlobMeta meta) throws KeyAlreadyExistsException {
        LOG.debug("Creating Blob for key {}", key);
        validateKey(key);
        if (fbs.exists(DATA_PREFIX + key)) {
            throw new KeyAlreadyExistsException(key);
        }
        BlobStoreFileOutputStream mOut = null;
        try {
            mOut = new BlobStoreFileOutputStream(fbs.write(META_PREFIX + key, true));
            mOut.write(JStormUtils.thriftSerialize(meta));
            mOut.close();
            mOut = null;
            return new BlobStoreFileOutputStream(fbs.write(DATA_PREFIX + key, true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (mOut != null) {
                try {
                    mOut.cancel();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public AtomicOutputStream updateBlob(String key) throws KeyNotFoundException {
        validateKey(key);
        checkForBlobOrDownload(key);
        getStoredBlobMeta(key);
        try {
            return new BlobStoreFileOutputStream(fbs.write(DATA_PREFIX + key, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SettableBlobMeta getStoredBlobMeta(String key) throws KeyNotFoundException {
        InputStream in = null;
        try {
            LocalFsBlobStoreFile pf = fbs.read(META_PREFIX + key);
            try {
                in = pf.getInputStream();
            } catch (FileNotFoundException fnf) {
                throw new KeyNotFoundException(key);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[2048];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            in.close();
            in = null;
            return JStormUtils.thriftDeserialize(SettableBlobMeta.class, out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws KeyNotFoundException {
        validateKey(key);
        if (!checkForBlobOrDownload(key)) {
            checkForBlobUpdate(key);
        }
        SettableBlobMeta meta = getStoredBlobMeta(key);
        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(meta);
        try {
            LocalFsBlobStoreFile pf = fbs.read(DATA_PREFIX + key);
            rbm.set_version(pf.getModTime());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rbm;
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta) throws KeyNotFoundException {
        validateKey(key);
        checkForBlobOrDownload(key);
        SettableBlobMeta orig = getStoredBlobMeta(key);
        BlobStoreFileOutputStream mOut = null;
        try {
            mOut = new BlobStoreFileOutputStream(fbs.write(META_PREFIX + key, false));
            mOut.write(JStormUtils.thriftSerialize(meta));
            mOut.close();
            mOut = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (mOut != null) {
                try {
                    mOut.cancel();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public void deleteBlob(String key) throws KeyNotFoundException {
        validateKey(key);
        checkForBlobOrDownload(key);
        getStoredBlobMeta(key);
        try {
            fbs.deleteKey(DATA_PREFIX + key);
            fbs.deleteKey(META_PREFIX + key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStreamWithMeta getBlob(String key) throws KeyNotFoundException {
        validateKey(key);
        if (!checkForBlobOrDownload(key)) {
            checkForBlobUpdate(key);
        }
        getStoredBlobMeta(key);
        try {
            return new BlobStoreFileInputStream(fbs.read(DATA_PREFIX + key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<String> listKeys() {
        try {
            return new KeyTranslationIterator(fbs.listKeys(), DATA_PREFIX);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        if (zkClient != null) {
            zkClient.close();
        }
        if (fbs != null) {
            fbs.shutdown();
        }
    }

    @Override
    public int getBlobReplication(String key) throws Exception {
        validateKey(key);
        getStoredBlobMeta(key);
        if (zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + key) == null) {
            return 0;
        }
        return zkClient.getChildren().forPath(BLOBSTORE_SUBTREE + key).size();
    }

    @Override
    public int updateBlobReplication(String key, int replication) throws KeyNotFoundException {
        throw new UnsupportedOperationException("For local file system blob store the update blobs function does not work. " +
                "Please use HDFS blob store to make this feature available.");
    }

    //This additional check and download is for nimbus high availability in case you have more than one nimbus
    public boolean checkForBlobOrDownload(String key) {
        boolean checkBlobDownload = false;
        long start = System.currentTimeMillis();
        ReentrantLock lock = getLockForKey(key);
        try {
            lock.lock();
            long getKeyStart = System.currentTimeMillis();
            List<String> keyList = BlobStoreUtils.getKeyListFromBlobStore(this);
            LOG.info("list blob keys, size:{}, cost:{}", keyList.size(), System.currentTimeMillis() - getKeyStart);

            if (!keyList.contains(key)) {
                if (zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + key) != null) {
                    Set<NimbusInfo> nimbusSet = BlobStoreUtils.getNimbodesWithLatestSequenceNumberOfBlob(zkClient, key);
                    Set<NimbusInfo> filterNimbusSet = new HashSet<>();
                    for (NimbusInfo nimbusInfo : nimbusSet) {
                        if (!nimbusInfo.getHostPort().equals(this.nimbusInfo.getHostPort())) {
                            filterNimbusSet.add(nimbusInfo);
                        }
                    }
                    if (BlobStoreUtils.downloadMissingBlob(conf, this, key, filterNimbusSet)) {
                        LOG.debug("Updating blobs state");
                        BlobStoreUtils.createStateInZookeeper(conf, key, nimbusInfo);
                        checkBlobDownload = true;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            unlockForKey(key, lock);
        }
        LOG.debug("checkForBlobOrDownload, key:{}, cost:{}", key, System.currentTimeMillis() - start);
        return checkBlobDownload;
    }

    public void checkForBlobUpdate(String key) {
        long start = System.currentTimeMillis();
        ReentrantLock lock = getLockForKey(key);
        try {
            lock.lock();
            BlobStoreUtils.updateKeyForBlobStore(conf, this, zkClient, key, nimbusInfo);
        } finally {
            unlockForKey(key, lock);
        }
        LOG.debug("checkForBlobUpdate, key:{}, cost:{}", key, System.currentTimeMillis() - start);
    }

    public void fullCleanup(long age) throws IOException {
        fbs.fullCleanup(age);
    }

    private ReentrantLock getLockForKey(String key) {
        ReentrantLock lock = new ReentrantLock();
        ReentrantLock existing = topologyLock.putIfAbsent(key, lock);
        ReentrantLock ret = (existing == null) ? lock : existing;
        LOG.debug("get lock for key:{}, lock:{}", key, ret);
        return ret;
    }

    private void unlockForKey(String key, ReentrantLock lock) {
        LOG.debug("unlock for key:{}, lock:{}", key, lock);
        lock.unlock();
        synchronized (topologyLock) {
            topologyLock.remove(key);
        }
    }
}
