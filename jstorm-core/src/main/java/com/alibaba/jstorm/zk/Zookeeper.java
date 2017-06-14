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
package com.alibaba.jstorm.zk;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.DefaultWatcherCallBack;
import com.alibaba.jstorm.callback.WatcherCallBack;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

/**
 * a simple ZK wrapper
 *
 * @author yannian
 */
public class Zookeeper {

    private static Logger LOG = LoggerFactory.getLogger(Zookeeper.class);

    public CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root) {
        return mkClient(conf, servers, port, root, new DefaultWatcherCallBack());
    }

    /**
     * connect ZK, register watchers
     */
    public CuratorFramework mkClient(Map conf, List<String> servers, Object port,
                                     String root, final WatcherCallBack watcher) {

        CuratorFramework fk = Utils.newCurator(conf, servers, port, root);

        fk.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework _fk, CuratorEvent e) throws Exception {
                if (e.getType().equals(CuratorEventType.WATCHED)) {
                    WatchedEvent event = e.getWatchedEvent();

                    watcher.execute(event.getState(), event.getType(), event.getPath());
                }

            }
        });

        fk.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String msg, Throwable error) {
                String errmsg = "Unrecoverable zookeeper error, halting process: " + msg;
                LOG.error(errmsg, error);
                JStormUtils.halt_process(1, "Unrecoverable zookeeper error");

            }
        });
        fk.start();
        return fk;
    }

    public String createNode(CuratorFramework zk, String path, byte[] data, org.apache.zookeeper.CreateMode mode)
            throws Exception {
        String normPath = PathUtils.normalize_path(path);
        return zk.create().withMode(mode).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(normPath, data);
    }

    public String createNode(CuratorFramework zk, String path, byte[] data) throws Exception {
        return createNode(zk, path, data, org.apache.zookeeper.CreateMode.PERSISTENT);
    }

    public boolean existsNode(CuratorFramework zk, String path, boolean watch) throws Exception {
        Stat stat;
        if (watch) {
            stat = zk.checkExists().watched().forPath(PathUtils.normalize_path(path));
        } else {
            stat = zk.checkExists().forPath(PathUtils.normalize_path(path));
        }
        return stat != null;
    }

    public void deleteNode(CuratorFramework zk, String path) throws Exception {
        zk.delete().forPath(PathUtils.normalize_path(path));
    }

    public void mkdirs(CuratorFramework zk, String path) throws Exception {
        String normPath = PathUtils.normalize_path(path);

        // the node is "/"
        if (normPath.equals("/")) {
            return;
        }

        // the node exist
        if (existsNode(zk, normPath, false)) {
            return;
        }

        mkdirs(zk, PathUtils.parent_path(normPath));
        try {
            createNode(zk, normPath, JStormUtils.barr((byte) 7), org.apache.zookeeper.CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            // this exception can be raised when multiple clients are making the same dir at the same time
            LOG.warn("zookeeper mkdir for path" + path, e);
        }

    }

    public Integer getVersion(CuratorFramework zk, String path, boolean watch) throws Exception {
        String normPath = PathUtils.normalize_path(path);
        Stat stat;
        if (existsNode(zk, normPath, watch)) {
            if (watch) {
                stat = zk.checkExists().watched().forPath(PathUtils.normalize_path(path));
            } else {
                stat = zk.checkExists().forPath(PathUtils.normalize_path(path));
            }
            return stat.getVersion();
        }

        return null;
    }

    public byte[] getData(CuratorFramework zk, String path, boolean watch) throws Exception {
        String normPath = PathUtils.normalize_path(path);
        try {
            if (existsNode(zk, normPath, watch)) {
                if (watch) {
                    return zk.getData().watched().forPath(normPath);
                } else {
                    return zk.getData().forPath(normPath);
                }
            }
        } catch (KeeperException e) {
            LOG.error("zookeeper getdata for path" + path, e);
        }

        return null;
    }

    public List<String> getChildren(CuratorFramework zk, String path, boolean watch) throws Exception {
        String normPath = PathUtils.normalize_path(path);

        if (watch) {
            return zk.getChildren().watched().forPath(normPath);
        } else {
            return zk.getChildren().forPath(normPath);
        }
    }

    public Stat setData(CuratorFramework zk, String path, byte[] data) throws Exception {
        String normPath = PathUtils.normalize_path(path);
        return zk.setData().forPath(normPath, data);
    }

    public boolean exists(CuratorFramework zk, String path, boolean watch) throws Exception {
        return existsNode(zk, path, watch);
    }

    public void syncPath(CuratorFramework zk, String path) throws Exception {
        zk.sync().forPath(Utils.normalize_path(path));
    }


    public void deleteRecursive(CuratorFramework zk, String path) throws Exception {
        String normPath = PathUtils.normalize_path(path);

        if (existsNode(zk, normPath, false)) {
            zk.delete().guaranteed().deletingChildrenIfNeeded().forPath(normPath);
        }
    }

    public static Factory mkInprocessZookeeper(String localDir, int port)
            throws IOException, InterruptedException {
        LOG.info("Starting in-process zookeeper at port " + port + " and dir " + localDir);
        File localFile = new File(localDir);
        ZooKeeperServer zk = new ZooKeeperServer(localFile, localFile, 2000);
        Factory factory = new Factory(new InetSocketAddress(port), 0);
        factory.startup(zk);
        return factory;
    }

    public void shutdownInprocessZookeeper(Factory handle) {
        handle.shutdown();
    }

}
