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
package com.alibaba.jstorm.cluster;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.callback.ClusterStateCallback;
import com.alibaba.jstorm.callback.WatcherCallBack;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.zk.Zookeeper;

/**
 * All ZK interface implementation
 *
 * @author yannian.mu
 */
public class DistributedClusterState implements ClusterState {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedClusterState.class);

    private final Zookeeper zkObj = new Zookeeper();
    private CuratorFramework zk;
    private final WatcherCallBack watcher;

    private final ConcurrentHashMap<UUID, ClusterStateCallback> callbacks = new ConcurrentHashMap<>();

    private final Map<Object, Object> conf;
    private final AtomicBoolean active;

    private JStormCache zkCache;

    public DistributedClusterState(Map<Object, Object> conf) throws Exception {
        this.conf = conf;

        // just mkdir STORM_ZOOKEEPER_ROOT dir
        CuratorFramework _zk = mkZk();
        String path = String.valueOf(this.conf.get(Config.STORM_ZOOKEEPER_ROOT));
        zkObj.mkdirs(_zk, path);
        _zk.close();

        active = new AtomicBoolean(true);

        watcher = new WatcherCallBack() {
            @Override
            public void execute(KeeperState state, EventType type, String path) {
                if (active.get()) {
                    if (!(state.equals(KeeperState.SyncConnected))) {
                        LOG.warn("Received event " + state + ":" + type + ":" + path + " with disconnected Zookeeper.");
                    } else {
                        LOG.info("Received event " + state + ":" + type + ":" + path);
                    }

                    if (!type.equals(EventType.None)) {
                        for (Entry<UUID, ClusterStateCallback> e : callbacks.entrySet()) {
                            ClusterStateCallback fn = e.getValue();
                            fn.execute(type, path);
                        }
                    }
                }
            }
        };
        zk = null;
        zk = mkZk(watcher);

    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mkZk() throws IOException {
        return zkObj.mkClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS), conf.get(Config.STORM_ZOOKEEPER_PORT), "");
    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mkZk(WatcherCallBack watcher) throws NumberFormatException, IOException {
        return zkObj.mkClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS), conf.get(Config.STORM_ZOOKEEPER_PORT),
                String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT)), watcher);
    }

    @Override
    public void close() {
        this.active.set(false);
        zk.close();
    }

    @Override
    public void delete_node(String path) throws Exception {
        if (zkCache != null) {
            zkCache.remove(path);
        }
        zkObj.deleteRecursive(zk, path);
    }

    @Override
    public List<String> get_children(String path, boolean watch) throws Exception {
        return zkObj.getChildren(zk, path, watch);
    }

    @Override
    public byte[] get_data(String path, boolean watch) throws Exception {
        byte[] ret = null;
        if (!watch && zkCache != null) {
            ret = (byte[]) zkCache.get(path);
        }
        if (ret != null) {
            return ret;
        }

        ret = zkObj.getData(zk, path, watch);
        if (zkCache != null) {
            zkCache.put(path, ret);
        }

        return ret;
    }

    /**
     * Note that get_version doesn't use zkCache avoid to conflict with get_data
     */
    @Override
    public Integer get_version(String path, boolean watch) throws Exception {
        return zkObj.getVersion(zk, path, watch);
    }

    @Override
    public byte[] get_data_sync(String path, boolean watch) throws Exception {
        byte[] ret;
        ret = zkObj.getData(zk, path, watch);
        if (zkCache != null && ret != null) {
            zkCache.put(path, ret);
        }
        return ret;
    }

    @Override
    public void sync_path(String path) throws Exception {
        zkObj.syncPath(zk, path);
    }

    @Override
    public void mkdirs(String path) throws Exception {
        zkObj.mkdirs(zk, path);
    }

    @Override
    public void set_data(String path, byte[] data) throws Exception {
        if (data.length > (JStormUtils.SIZE_1_K * 800)) {
            throw new Exception("Writing 800k+ data into ZK is not allowed!, data size is " + data.length);
        }
        if (zkObj.exists(zk, path, false)) {
            zkObj.setData(zk, path, data);
        } else {
            zkObj.mkdirs(zk, PathUtils.parent_path(path));
            zkObj.createNode(zk, path, data, CreateMode.PERSISTENT);
        }

        if (zkCache != null) {
            zkCache.put(path, data);
        }
    }

    @Override
    public void set_ephemeral_node(String path, byte[] data) throws Exception {
        zkObj.mkdirs(zk, PathUtils.parent_path(path));
        if (zkObj.exists(zk, path, false)) {
            zkObj.setData(zk, path, data);
        } else {
            zkObj.createNode(zk, path, data, CreateMode.EPHEMERAL);
        }

        if (zkCache != null) {
            zkCache.put(path, data);
        }
    }

    @Override
    public UUID register(ClusterStateCallback callback) {
        UUID id = UUID.randomUUID();
        this.callbacks.put(id, callback);
        return id;
    }

    @Override
    public ClusterStateCallback unregister(UUID id) {
        return this.callbacks.remove(id);
    }

    @Override
    public boolean node_existed(String path, boolean watch) throws Exception {
        return zkObj.existsNode(zk, path, watch);
    }

    @Override
    public void tryToBeLeader(String path, byte[] host) throws Exception {
        zkObj.createNode(zk, path, host, CreateMode.EPHEMERAL);
    }

    public JStormCache getZkCache() {
        return zkCache;
    }

    public void setZkCache(JStormCache zkCache) {
        this.zkCache = zkCache;
    }

}
