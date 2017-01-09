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
package com.alibaba.jstorm.schedule;

import java.util.*;

import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.blobstore.BlobSynchronizer;
import com.alibaba.jstorm.blobstore.LocalFsBlobStore;
import com.alibaba.jstorm.callback.Callback;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class FollowerRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerRunnable.class);

    private NimbusData data;

    private int sleepTime;

    private volatile boolean state = true;

    private RunnableCallback blobSyncCallback;

    private Callback leaderCallback;

    private final String hostPort;

    public static final String NIMBUS_DIFFER_COUNT_ZK = "nimbus.differ.count.zk";

    public static final Integer SLAVE_NIMBUS_WAIT_TIME = 60;

    @SuppressWarnings("unchecked")
    public FollowerRunnable(final NimbusData data, int sleepTime, Callback leaderCallback) {
        this.data = data;
        this.sleepTime = sleepTime;
        this.leaderCallback = leaderCallback;
        boolean isLocaliP;
        if (!ConfigExtension.isNimbusUseIp(data.getConf())) {
            this.hostPort = NetWorkUtils.hostname() + ":" + String.valueOf(Utils.getInt(data.getConf().get(Config.NIMBUS_THRIFT_PORT)));
            isLocaliP = NetWorkUtils.hostname().equals("localhost");
        } else {
            this.hostPort = NetWorkUtils.ip() + ":" + String.valueOf(Utils.getInt(data.getConf().get(Config.NIMBUS_THRIFT_PORT)));
            isLocaliP = NetWorkUtils.ip().equals("127.0.0.1");
        }
        try {
            if (isLocaliP) {
                throw new Exception("the hostname which Nimbus get is localhost");
            }
        } catch (Exception e1) {
            LOG.error("get nimbus host error!", e1);
            throw new RuntimeException(e1);
        }

        try {
            data.getStormClusterState().update_nimbus_slave(hostPort, data.uptime());
            data.getStormClusterState().update_nimbus_detail(hostPort, null);
        } catch (Exception e) {
            LOG.error("register nimbus host fail!", e);
            throw new RuntimeException();
        }
        StormClusterState zkClusterState = data.getStormClusterState();
        try{
            if (!zkClusterState.leader_existed()) {
                this.tryToBeLeader(data.getConf());
            }
        }catch (Exception e){
            LOG.error("register detail of nimbus fail!", e);
            throw new RuntimeException();
        }
        try {
            if (!zkClusterState.leader_existed()) {
                this.tryToBeLeader(data.getConf());
            }
        } catch (Exception e1) {
            try {
                data.getStormClusterState().unregister_nimbus_host(hostPort);
                data.getStormClusterState().unregister_nimbus_detail(hostPort);
            }catch (Exception e2){
                LOG.info("due to task errors, so remove register nimbus infomation" );
            }finally {
                // TODO Auto-generated catch block
                LOG.error("try to be leader error.", e1);
                throw new RuntimeException(e1);
            }
        }
        blobSyncCallback = new RunnableCallback() {
            @Override
            public void run() {
                blobSync();
            }
        };
        if (data.getBlobStore() instanceof LocalFsBlobStore) {
            try {
                // register call back for blob-store
                data.getStormClusterState().blobstore(blobSyncCallback);
                setupBlobstore();
            } catch (Exception e) {
                LOG.error("setup blob store error", e);
            }
        }
    }


    // sets up blobstore state for all current keys
    private void setupBlobstore() throws Exception {
        BlobStore blobStore = data.getBlobStore();
        StormClusterState clusterState = data.getStormClusterState();
        Set<String> localSetOfKeys = Sets.newHashSet(blobStore.listKeys());
        Set<String> allKeys = Sets.newHashSet(clusterState.active_keys());
        Set<String> localAvailableActiveKeys = Sets.intersection(localSetOfKeys, allKeys);
        // keys on local but not on zk, we will delete it
        Set<String> keysToDelete = Sets.difference(localSetOfKeys, allKeys);
        LOG.debug("deleting keys not on the zookeeper {}", keysToDelete);
        for (String key : keysToDelete) {
            blobStore.deleteBlob(key);
        }
        //    (log-debug "Creating list of key entries for blobstore inside zookeeper" all-keys "local" locally-available-active-keys)
        LOG.debug("Creating list of key entries for blobstore inside zookeeper {} local {}", allKeys, localAvailableActiveKeys);
        for (String key : localAvailableActiveKeys) {
            int versionForKey = BlobStoreUtils.getVersionForKey(key, data.getNimbusHostPortInfo(), data.getConf());
            clusterState.setup_blobstore(key, data.getNimbusHostPortInfo(), versionForKey);
        }
    }

    public boolean isLeader(String zkMaster) {
        if (StringUtils.isBlank(zkMaster) == true) {
            return false;
        }

        if (hostPort.equalsIgnoreCase(zkMaster) == true) {
            return true;
        }

        // Two nimbus running on the same node isn't allowed
        // so just checks ip is enough here
        String[] part = zkMaster.split(":");
        return NetWorkUtils.equals(part[0], NetWorkUtils.ip());
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        LOG.info("Follower Thread starts!");
        while (state) {
            StormClusterState zkClusterState = data.getStormClusterState();
            try {
                Thread.sleep(sleepTime);
                if (!zkClusterState.leader_existed()) {
                    this.tryToBeLeader(data.getConf());
                    continue;
                }

                String master = zkClusterState.get_leader_host();
                boolean isZkLeader = isLeader(master);
                if (isZkLeader) {
                    if (!data.isLeader()) {
                        zkClusterState.unregister_nimbus_host(hostPort);
                        zkClusterState.unregister_nimbus_detail(hostPort);
                        data.setLeader(true);
                        leaderCallback.execute();
                    }
                    continue;
                } else {
                    if (data.isLeader()) {
                        LOG.info("New ZK master is " + master);
                        JStormUtils.halt_process(1, "Lose ZK master node, halt process");
                        return;
                    }
                }

                // here the nimbus is not leader
                if (data.getBlobStore() instanceof LocalFsBlobStore){
                    blobSync();
                }
                zkClusterState.update_nimbus_slave(hostPort, data.uptime());
                update_nimbus_detail();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                continue;
            } catch (Exception e) {
                if (state) {
                    LOG.error("Unknow exception ", e);
                }
            }
        }
        LOG.info("Follower Thread has closed!");
    }

    public void clean() {
        state = false;
    }


    private synchronized void blobSync(){
        if (!data.isLeader()) {
            try {
                BlobStore blobStore = data.getBlobStore();
                StormClusterState clusterState = data.getStormClusterState();
                Set<String> localKeys = Sets.newHashSet(blobStore.listKeys());
                Set<String> zkKeys = Sets.newHashSet(clusterState.blobstore(blobSyncCallback));
                BlobSynchronizer blobSynchronizer = new BlobSynchronizer(blobStore, data.getConf());
                blobSynchronizer.setNimbusInfo(data.getNimbusHostPortInfo());
                blobSynchronizer.setBlobStoreKeySet(localKeys);
                blobSynchronizer.setZookeeperKeySet(zkKeys);
                blobSynchronizer.syncBlobs();
            } catch (Exception e) {
                LOG.error("blob sync error", e);
            }
        }
    }


    private void tryToBeLeader(final Map conf) throws Exception {
        boolean allowed = check_nimbus_priority();
        
        if (allowed){
            RunnableCallback masterCallback = new RunnableCallback() {
                @Override
                public void run() {
                    try {
                        tryToBeLeader(conf);
                    } catch (Exception e) {
                        LOG.error("To be master error", e);
                        JStormUtils.halt_process(30, "Cant't to be master" + e.getMessage());
                    }
                }
            };
            LOG.info("This nimbus can be  leader");
            data.getStormClusterState().try_to_be_leader(Cluster.MASTER_SUBTREE, hostPort, masterCallback);
        }else {
        	LOG.info("This nimbus can't be leader");
        }
    }
    /**
     * Compared with other nimbus ,get priority of this nimbus
     *
     * @throws Exception
     */
    private  boolean check_nimbus_priority() throws Exception {
    	
    	int gap = update_nimbus_detail();
    	if (gap == 0) {
    		return true;
    	}

        int left = SLAVE_NIMBUS_WAIT_TIME;
        while (left > 0) {
            LOG.info("nimbus.differ.count.zk is {}, so after {} seconds, nimbus will try to be Leader!", gap, left);
            Thread.sleep(10 * 1000);
            left -= 10;
        }

        StormClusterState zkClusterState = data.getStormClusterState();

        List<String> followers = zkClusterState.list_dirs(Cluster.NIMBUS_SLAVE_DETAIL_SUBTREE, false);
        if (followers == null || followers.size() == 0) {
            return false;
        }

        for (String follower : followers) {
            if (follower != null && !follower.equals(hostPort)) {
                Map bMap = zkClusterState.get_nimbus_detail(follower, false);
                if (bMap != null){
                    Object object = bMap.get(NIMBUS_DIFFER_COUNT_ZK);
                    if (object != null && (JStormUtils.parseInt(object)) < gap){
                    	LOG.info("Current node can't be leader, due to {} has higher priority", follower);
                        return false;
                    }
                }
            }
        }
        
        return true;
    }

    private int update_nimbus_detail() throws Exception {
        //update count = count of zk's binary files - count of nimbus's binary files
        StormClusterState zkClusterState = data.getStormClusterState();

        // if we use other blobstore, such as HDFS, all nimbus slave can be leader
        // but if we use local blobstore, we should count topologies files
        int diffCount = 0;
        if (data.getBlobStore() instanceof LocalFsBlobStore) {

            Set<String> keysOnZk = Sets.newHashSet(zkClusterState.active_keys());
            Set<String> keysOnLocal = Sets.newHashSet(data.getBlobStore().listKeys());
            // we count number of keys which is on zk but not on local
            diffCount = Sets.difference(keysOnZk, keysOnLocal).size();
        }

        Map mtmp = zkClusterState.get_nimbus_detail(hostPort, false);
        if (mtmp == null){
            mtmp = new HashMap();
        }
        mtmp.put(NIMBUS_DIFFER_COUNT_ZK, diffCount);
        zkClusterState.update_nimbus_detail(hostPort, mtmp);
        LOG.debug("update nimbus's detail " + mtmp);
        return diffCount;
    }
    /**
     * Check whether current node is master or not
     * 
     * @throws Exception
     */
    private void checkOwnMaster() throws Exception {
        int retry_times = 10;

        StormClusterState zkClient = data.getStormClusterState();
        for (int i = 0; i < retry_times; i++, JStormUtils.sleepMs(sleepTime)) {

            if (zkClient.leader_existed() == false) {
                continue;
            }

            String zkHost = zkClient.get_leader_host();
            if (hostPort.equals(zkHost) == true) {
                // current process own master
                return;
            }
            LOG.warn("Current Nimbus has start thrift, but fail to own zk master :" + zkHost);
        }

        // current process doesn't own master
        String err = "Current Nimubs fail to own nimbus_master, should halt process";
        LOG.error(err);
        JStormUtils.halt_process(0, err);

    }

}
