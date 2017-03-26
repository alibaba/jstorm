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

package backtype.storm.command;

import backtype.storm.GenericOptionsParser;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.blobstore.LocalFsBlobStore;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.PathUtils;
import com.google.common.collect.Sets;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * this tool is used to migrate stormdist to blobstore
 * when upgrade jstorm cluster at the first time
 *
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class blobstore {
    public static final String MIGRATE_FLAG = "-m";
    public static final String MIGRATE_FULL_FLAG = "--migrate";
    public static final String CLEAN_FULL_FLAG = "--cleanup";
    public static final String ALL = "all";


    private static Map conf;
    private static NimbusInfo nimbusInfo;
    private static BlobStore blobStore;
    private static StormClusterState clusterState;
    private static boolean isLocalBlobStore;

    public static void init() throws Exception {
        conf = Utils.readStormConfig();
        nimbusInfo = NimbusInfo.fromConf(conf);
        blobStore = BlobStoreUtils.getNimbusBlobStore(conf, nimbusInfo);
        clusterState = Cluster.mk_storm_cluster_state(conf);
        isLocalBlobStore = blobStore instanceof LocalFsBlobStore;
    }

    public static void migrateOldTopologyFiles() throws Exception {
        init();
        String stormRoot = StormConfig.masterStormdistRoot(conf);
        List<String> topologies = PathUtils.read_dir_contents(stormRoot);
        Set<String> activeKeys = Sets.newHashSet(blobStore.listKeys());

        for (String topologyId : topologies) {
            try {
                setupStormCode(conf, topologyId, StormConfig.masterStormdistRoot(conf, topologyId), activeKeys);
            } catch (Exception e) {
                System.out.println("Create blobstore for topology error");
                e.printStackTrace();
            }
        }

    }

    /**
     * create local topology files in blobstore and sync metadata to zk
     */
    private static void setupStormCode(Map conf, String topologyId, String topologyRoot, Set<String> activeKeys)
            throws Exception {
        Map<String, String> blobKeysToLocation = getBlobKeysToLocation(topologyRoot, topologyId);
        for (Map.Entry<String, String> entry : blobKeysToLocation.entrySet()) {
            String key = entry.getKey();
            String location = entry.getValue();
            if (!activeKeys.contains(key)) {
                blobStore.createBlob(key, new FileInputStream(location), new SettableBlobMeta());
                if (isLocalBlobStore) {
                    int versionInfo = BlobStoreUtils.getVersionForKey(key, nimbusInfo, conf);
                    clusterState.setup_blobstore(key, nimbusInfo, versionInfo);
                }
            }
        }

        System.out.println("Successfully create blobstore for topology " + topologyId);
    }

    private static Map<String, String> getBlobKeysToLocation(String topologyRoot, String topologyId) throws IOException {
        Map<String, String> keys = new HashMap<>();
        String jarKey = StormConfig.master_stormjar_key(topologyId);
        String codeKey = StormConfig.master_stormcode_key(topologyId);
        String confKey = StormConfig.master_stormconf_key(topologyId);
        keys.put(jarKey, StormConfig.stormjar_path(topologyRoot));
        keys.put(codeKey, StormConfig.stormcode_path(topologyRoot));
        keys.put(confKey, StormConfig.stormconf_path(topologyRoot));

        Map stormConf = StormConfig.read_topology_conf(topologyRoot, topologyId);
        if (stormConf != null) {
            List<String> libs = (List<String>) stormConf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
            if (libs != null) {
                for (String libName : libs) {
                    keys.put(StormConfig.master_stormlib_key(topologyId, libName),
                            StormConfig.stormlib_path(topologyRoot, libName));
                }
            }
        }
        return keys;
    }

    private static void cleanupBlobs(String topology) throws Exception {
        init();
        if (topology.equalsIgnoreCase(ALL)) {
            for (String key : clusterState.active_keys()) {
                clusterState.remove_blobstore_key(key);
                clusterState.remove_key_version(key);
            }
        } else {
            for (String key : clusterState.active_keys()) {
                if (key.startsWith(topology + "-")) {
                    clusterState.remove_blobstore_key(key);
                    clusterState.remove_key_version(key);
                }
            }
        }

    }

    private static void printErrorInfo() {
        System.out.println("Error: Invalid parameters!");
        System.out.println("USAGE: jstorm blobstore -m");
        System.out.println("\t-m, --migrate\t\tmigrate stormdist to blobstore");
    }

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            printErrorInfo();
            return;
        }

        String flag = args[0].toLowerCase();

        try {
            if (flag.equals(MIGRATE_FLAG) || flag.equals(MIGRATE_FULL_FLAG)) {
                migrateOldTopologyFiles();
            } else if (flag.equals(CLEAN_FULL_FLAG)) {
                String topology = ALL;
                if (args.length >= 2) {
                    topology = args[1];
                }
                cleanupBlobs(topology);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
