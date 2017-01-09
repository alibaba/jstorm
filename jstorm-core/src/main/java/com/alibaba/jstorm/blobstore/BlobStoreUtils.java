/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.blobstore;

import backtype.storm.Config;
import backtype.storm.GenericOptionsParser;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlobStoreUtils {
    private static final String BLOBSTORE_SUBTREE = "/blobstore";
    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreUtils.class);

    public static CuratorFramework createZKClient(Map conf) throws Exception {
        CuratorFramework zkClient = null;
        try {
            List<String> zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
            Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
            zkClient = Utils.newCurator(conf, zkServers, port, (String) conf.get(Config.STORM_ZOOKEEPER_ROOT));
            zkClient.start();
        }catch (Exception e){
            if (zkClient != null) {
                zkClient.close();
                zkClient = null;
            }
            throw e;
        }
        return zkClient;
    }

//    public static Subject getNimbusSubject() {
//        Subject subject = new Subject();
//        subject.getPrincipals().add(new NimbusPrincipal());
//        return subject;
//    }

    // Normalize state
    public static BlobKeySequenceInfo normalizeNimbusHostPortSequenceNumberInfo(String nimbusSeqNumberInfo) {
        BlobKeySequenceInfo keySequenceInfo = new BlobKeySequenceInfo();
        int lastIndex = nimbusSeqNumberInfo.lastIndexOf("-");
        keySequenceInfo.setNimbusHostPort(nimbusSeqNumberInfo.substring(0, lastIndex));
        keySequenceInfo.setSequenceNumber(nimbusSeqNumberInfo.substring(lastIndex + 1));
        return keySequenceInfo;
    }

    // Check for latest sequence number of a key inside zookeeper and return nimbodes containing the latest sequence number
    public static Set<NimbusInfo> getNimbodesWithLatestSequenceNumberOfBlob(CuratorFramework zkClient, String key) throws Exception {
        List<String> stateInfoList = zkClient.getChildren().forPath(BLOBSTORE_SUBTREE + "/" + key);
        Set<NimbusInfo> nimbusInfoSet = new HashSet<NimbusInfo>();
        int latestSeqNumber = getLatestSequenceNumber(stateInfoList);
        LOG.debug("getNimbodesWithLatestSequenceNumberOfBlob stateInfo {} version {}", stateInfoList, latestSeqNumber);
        // Get the nimbodes with the latest version
        for (String state : stateInfoList) {
            BlobKeySequenceInfo sequenceInfo = normalizeNimbusHostPortSequenceNumberInfo(state);
            if (latestSeqNumber == Integer.parseInt(sequenceInfo.getSequenceNumber())) {
                nimbusInfoSet.add(NimbusInfo.parse(sequenceInfo.getNimbusHostPort()));
            }
        }
        LOG.debug("nimbusInfoList {}", nimbusInfoSet);
        return nimbusInfoSet;
    }

    // Get sequence number details from latest sequence number of the blob
    public static int getLatestSequenceNumber(List<String> stateInfoList) {
        int seqNumber = 0;
        // Get latest sequence number of the blob present in the zookeeper --> possible to refactor this piece of code
        for (String state : stateInfoList) {
            BlobKeySequenceInfo sequenceInfo = normalizeNimbusHostPortSequenceNumberInfo(state);
            int currentSeqNumber = Integer.parseInt(sequenceInfo.getSequenceNumber());
            if (seqNumber < currentSeqNumber) {
                seqNumber = currentSeqNumber;
                LOG.debug("Sequence Info {}", seqNumber);
            }
        }
        LOG.debug("Latest Sequence Number {}", seqNumber);
        return seqNumber;
    }

    // Download missing blobs from potential nimbodes
    public static boolean downloadMissingBlob(Map conf, BlobStore blobStore, String key, Set<NimbusInfo> nimbusInfos)
            throws TTransportException {
        NimbusClient client;
        ReadableBlobMeta rbm;
        ClientBlobStore remoteBlobStore;
        InputStreamWithMeta in;
        boolean isSuccess = false;
        LOG.debug("Download blob NimbusInfos {}", nimbusInfos);
        for (NimbusInfo nimbusInfo : nimbusInfos) {
            if (isSuccess) {
                break;
            }
            try {
                client = new NimbusClient(conf, nimbusInfo.getHost(), nimbusInfo.getPort(), null);
                rbm = client.getClient().getBlobMeta(key);
                remoteBlobStore = new NimbusBlobStore();
                remoteBlobStore.setClient(conf, client);
                in = remoteBlobStore.getBlob(key);
                blobStore.createBlob(key, in, rbm.get_settable());
                // if key already exists while creating the blob else update it
                Iterator<String> keyIterator = blobStore.listKeys();
                while (keyIterator.hasNext()) {
                    if (keyIterator.next().equals(key)) {
                        LOG.debug("Success creating key, {}", key);
                        isSuccess = true;
                        break;
                    }
                }
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            } catch (KeyAlreadyExistsException kae) {
                LOG.info("KeyAlreadyExistsException Key: {} {}", key, kae);
            } catch (KeyNotFoundException knf) {
                // Catching and logging KeyNotFoundException because, if
                // there is a subsequent update and delete, the non-leader
                // nimbodes might throw an exception.
                LOG.info("KeyNotFoundException Key: {} {}", key, knf);
            } catch (Exception exp) {
                // Logging an exception while client is connecting
                LOG.error("Exception ", exp);
            }
        }

        if (!isSuccess) {
            LOG.error("Could not download blob with key" + key);
        }
        return isSuccess;
    }

    public static boolean updateBlob(BlobStore blobStore, String key, byte[] data) throws IOException,
            KeyNotFoundException {
        boolean isSuccess = false;
        AtomicOutputStream out = null;
        try {
            out = blobStore.updateBlob(key);
            out.write(data);
            out.close();
            out = null;
            isSuccess = true;
        } finally {
            if (out != null) {
                out.cancel();
            }
        }
        return isSuccess;
    }

    public static boolean updateBlob(BlobStore blobStore, String key, InputStream inputStream) throws IOException,
            KeyNotFoundException, Exception {
        AtomicOutputStream out;
        out = blobStore.updateBlob(key);
        byte[] buffer = new byte[2048];
        int len = 0;
        while ((len = inputStream.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        if (out != null) {
            out.close();
        }
        return true;
    }

    // Download updated blobs from potential nimbodes
    public static boolean downloadUpdatedBlob(Map conf, BlobStore blobStore, String key, Set<NimbusInfo> nimbusInfos)
            throws TTransportException {
        NimbusClient client;
        ClientBlobStore remoteBlobStore;
        InputStreamWithMeta in;
        AtomicOutputStream out;
        boolean isSuccess = false;
        LOG.debug("Download blob NimbusInfos {}", nimbusInfos);
        for (NimbusInfo nimbusInfo : nimbusInfos) {
            if (isSuccess) {
                break;
            }
            try {
                client = new NimbusClient(conf, nimbusInfo.getHost(), nimbusInfo.getPort(), null);
                remoteBlobStore = new NimbusBlobStore();
                remoteBlobStore.setClient(conf, client);
                isSuccess = updateBlob(blobStore, key, remoteBlobStore.getBlob(key));
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            } catch (KeyNotFoundException knf) {
                // Catching and logging KeyNotFoundException because, if
                // there is a subsequent update and delete, the non-leader
                // nimbodes might throw an exception.
                LOG.info("KeyNotFoundException {}", knf);
            } catch (Exception exp) {
                // Logging an exception while client is connecting
                LOG.error("Exception {}", exp);
            }
        }

        if (!isSuccess) {
            LOG.error("Could not update the blob with key" + key);
        }
        return isSuccess;
    }

    // Get the list of keys from blobstore
    public static List<String> getKeyListFromBlobStore(BlobStore blobStore) throws Exception {
        Iterator<String> keys = blobStore.listKeys();
        List<String> keyList = new ArrayList<String>();
        if (keys != null) {
            while (keys.hasNext()) {
                keyList.add(keys.next());
            }
        }
        LOG.debug("KeyList from blobstore {}", keyList);
        return keyList;
    }

    public static void createStateInZookeeper(Map conf, String key, NimbusInfo nimbusInfo) throws TTransportException {
        ClientBlobStore cb = new NimbusBlobStore();
        cb.setClient(conf, new NimbusClient(conf, nimbusInfo.getHost(), nimbusInfo.getPort(), null));
        cb.createStateInZookeeper(key);
    }

    public static void updateKeyForBlobStore(Map conf, BlobStore blobStore, CuratorFramework zkClient, String key, NimbusInfo nimbusDetails) {
        try {
            // Most of clojure tests currently try to access the blobs using getBlob. Since, updateKeyForBlobStore
            // checks for updating the correct version of the blob as a part of nimbus ha before performing any
            // operation on it, there is a neccessity to stub several test cases to ignore this method. It is a valid
            // trade off to return if nimbusDetails which include the details of the current nimbus host port data are
            // not initialized as a part of the test. Moreover, this applies to only local blobstore when used along with
            // nimbus ha.
            if (nimbusDetails == null) {
                return;
            }
            boolean isListContainsCurrentNimbusInfo = false;
            List<String> stateInfo;
            if (zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + "/" + key) == null) {
                return;
            }
            stateInfo = zkClient.getChildren().forPath(BLOBSTORE_SUBTREE + "/" + key);
            LOG.debug("StateInfo for update {}", stateInfo);
            Set<NimbusInfo> nimbusInfoList = getNimbodesWithLatestSequenceNumberOfBlob(zkClient, key);

            for (NimbusInfo nimbusInfo : nimbusInfoList) {
                if (nimbusInfo.getHostPort().equals(nimbusDetails.getHostPort())) {
                    isListContainsCurrentNimbusInfo = true;
                    break;
                }
            }

            if (!isListContainsCurrentNimbusInfo && downloadUpdatedBlob(conf, blobStore, key, nimbusInfoList)) {
                LOG.debug("Updating state inside zookeeper for an update");
                createStateInZookeeper(conf, key, nimbusDetails);
            }
        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }

    public static ClientBlobStore getClientBlobStoreForSupervisor(Map conf) {
        ClientBlobStore store = (ClientBlobStore) Utils.newInstance(
                (String) conf.get(Config.SUPERVISOR_BLOBSTORE));
        store.prepare(conf);
        return store;
    }

    public static BlobStore getNimbusBlobStore(Map conf, NimbusInfo nimbusInfo) {
        return getNimbusBlobStore(conf, null, nimbusInfo);
    }

    public static BlobStore getNimbusBlobStore(Map conf, String baseDir, NimbusInfo nimbusInfo) {
        String type = (String) conf.get(Config.NIMBUS_BLOBSTORE);
        if (type == null) {
            type = LocalFsBlobStore.class.getName();
        }
        BlobStore store = (BlobStore) Utils.newInstance(type);
        HashMap nconf = new HashMap(conf);
        // only enable cleanup of blobstore on nimbus
        nconf.put(Config.BLOBSTORE_CLEANUP_ENABLE, Boolean.TRUE);
        store.prepare(nconf, baseDir, nimbusInfo);
        return store;
    }

    /**
     * Meant to be called only by the supervisor for stormjar/stormconf/stormcode files.
     *
     * @param key
     * @param localFile
     * @param cb
     * @throws KeyNotFoundException
     * @throws IOException
     */
    public static void downloadResourcesAsSupervisor(String key, String localFile,
                                                     ClientBlobStore cb, Map conf) throws KeyNotFoundException, IOException {
        if (cb instanceof NimbusBlobStore) {
            List<NimbusInfo> nimbusInfos = null;
            CuratorFramework zkClient = null;
            try {
                zkClient = BlobStoreUtils.createZKClient(conf);
                nimbusInfos = Lists.newArrayList(BlobStoreUtils.getNimbodesWithLatestSequenceNumberOfBlob(zkClient, key));
                Collections.shuffle(nimbusInfos);
            } catch (Exception e) {
                LOG.error("get available nimbus for blob key:{} error", e);
                return;
            }finally {
                if (zkClient != null) {
                    zkClient.close();
                    zkClient = null;
                }
            }
            if (nimbusInfos != null){
                for (NimbusInfo nimbusInfo : nimbusInfos) {
                    try {
                        NimbusClient nimbusClient = new NimbusClient(conf, nimbusInfo.getHost(), nimbusInfo.getPort());
                        cb.setClient(conf, nimbusClient);
                    } catch (TTransportException e) {
                        // ignore
                        continue;
                    }
                    LOG.info("download blob {} from nimbus {}:{}", key, nimbusInfo.getHost(), nimbusInfo.getPort());
                    downloadResourcesAsSupervisorDirect(key, localFile, cb);
                }
            }

        } else {
            downloadResourcesAsSupervisorDirect(key, localFile, cb);
        }
    }

    /**
     * Meant to be called only by the supervisor for stormjar/stormconf/stormcode files.
     *
     * @param key
     * @param localFile
     * @param cb
     * @throws KeyNotFoundException
     * @throws IOException
     */
    public static void downloadResourcesAsSupervisorDirect(String key, String localFile,
                                                           ClientBlobStore cb) throws KeyNotFoundException, IOException {
        final int MAX_RETRY_ATTEMPTS = 2;
        final int ATTEMPTS_INTERVAL_TIME = 100;
        for (int retryAttempts = 0; retryAttempts < MAX_RETRY_ATTEMPTS; retryAttempts++) {
            if (downloadResourcesAsSupervisorAttempt(cb, key, localFile)) {
                break;
            }
            Utils.sleep(ATTEMPTS_INTERVAL_TIME);
        }
    }

    private static boolean downloadResourcesAsSupervisorAttempt(ClientBlobStore cb, String key, String localFile) {
        boolean isSuccess = false;
        FileOutputStream out = null;
        InputStreamWithMeta in = null;
        try {
            out = new FileOutputStream(localFile);
            in = cb.getBlob(key);
            long fileSize = in.getFileLength();

            byte[] buffer = new byte[1024];
            int len;
            int downloadFileSize = 0;
            while ((len = in.read(buffer)) >= 0) {
                out.write(buffer, 0, len);
                downloadFileSize += len;
            }

            isSuccess = (fileSize == downloadFileSize);
        } catch (TException | IOException e) {
            LOG.error("An exception happened while downloading {} from blob store.", localFile, e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ignored) {
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ignored) {
            }
        }
        if (!isSuccess) {
            try {
                Files.deleteIfExists(Paths.get(localFile));
            } catch (IOException ex) {
                LOG.error("Failed trying to delete the partially downloaded {}", localFile, ex);
            }
        }
        return isSuccess;
    }

    public static boolean checkFileExists(String dir, String file) {
        return Files.exists(new File(dir, file).toPath());
    }

    /**
     * Filters keys based on the KeyFilter
     * passed as the argument.
     * @param filter KeyFilter
     * @param <R> Type
     * @return Set of filtered keys
     */
    public static <R> Set<R> filterAndListKeys(Iterator<R> keys, KeyFilter<R> filter) {
        Set<R> ret = new HashSet<R>();
        while (keys.hasNext()) {
            R key = keys.next();
            R filtered = filter.filter(key);
            if (filtered != null) {
                ret.add(filtered);
            }
        }
        return ret;
    }


    /**
     * topology ids in blobstore
     *
     * @param blobStore
     * @return
     */
    public static Set<String> code_ids(BlobStore blobStore) {
        return code_ids(blobStore.listKeys());
    }

    public static Set<String> code_ids(Iterator<String> iterator) {
        KeyFilter<String> keyFilter = new KeyFilter<String>() {
            @Override
            public String filter(String key) {
                Pattern p = Pattern.compile("^(.*)((-stormjar\\.jar)|(-stormcode\\.ser)|(-stormconf\\.ser)|(-lib-.*))$");
                Matcher matcher = p.matcher(key);
                if (matcher.matches()) {
                    return matcher.group(1);
                }
                return null;
            }
        };
        return Sets.newHashSet(filterAndListKeys(iterator, keyFilter));
    }

    // remove blob information in zk for the blobkey
    public static void cleanup_key(String blobKey, BlobStore blobStore, StormClusterState clusterState) {
        // we skip to clean up the non-exist keys
        if (blobKey.startsWith(JStormMetrics.NIMBUS_METRIC_KEY)
                || blobKey.startsWith(JStormMetrics.CLUSTER_METRIC_KEY)
                || blobKey.startsWith(JStormMetrics.SUPERVISOR_METRIC_KEY)) {
            return;
        }
        try {
            blobStore.deleteBlob(blobKey);
        } catch (Exception e) {
            LOG.warn("cleanup blob key {} error {}", blobKey, e);
        }

        try {
            if (blobStore instanceof LocalFsBlobStore) {
                clusterState.remove_blobstore_key(blobKey);
                clusterState.remove_key_version(blobKey);
            }
        } catch (Exception e) {
            LOG.warn("cleanup blob key {} error {}", blobKey, e);
        }
    }

    public static void cleanup_keys(List<String> deleteKeys, BlobStore blobStore, StormClusterState clusterState) {
        if (deleteKeys != null) {
            for (String key : deleteKeys) {
                cleanup_key(key, blobStore, clusterState);
            }
        }
    }

    public static int getVersionForKey(String key, NimbusInfo nimbusInfo, Map conf) {
        KeySequenceNumber version = new KeySequenceNumber(key, nimbusInfo);
        return version.getKeySequenceNumber(conf);
    }

    // get all key list (jar conf code lib) for the topology id, it only works in nimbus
    public static List<String> getKeyListFromId(NimbusData data, String topologyId)
            throws IOException, KeyNotFoundException {
        List<String> keys = new ArrayList<>();
        keys.add(StormConfig.master_stormjar_key(topologyId));
        keys.add(StormConfig.master_stormcode_key(topologyId));
        keys.add(StormConfig.master_stormconf_key(topologyId));


        Map stormConf = null;
        try {
            stormConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
        } catch (KeyNotFoundException e) {
            LOG.warn("can't find conf of topology {}", topologyId);
        }
        if (stormConf != null) {
            List<String> libs = (List<String>) stormConf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
            if (libs != null) {
                for (String libName : libs) {
                    keys.add(StormConfig.master_stormlib_key(topologyId, libName));
                }
            }
        }
        return keys;
    }
}
