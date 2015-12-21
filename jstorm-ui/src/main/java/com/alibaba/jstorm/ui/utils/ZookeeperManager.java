package com.alibaba.jstorm.ui.utils;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.ui.model.ClusterConfig;
import com.alibaba.jstorm.ui.model.ZookeeperNode;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.zk.Zookeeper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Donne (lindeqiang1988@gmail.com)
 */

@Service
public class ZookeeperManager {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperManager.class);
    private static Zookeeper zookeeper = new Zookeeper();
    private static ConcurrentMap<String, CuratorFramework> zkClients = new ConcurrentHashMap<>();
    private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static List<ZookeeperNode> listZKNodes(String clusterName, String path) {
        CuratorFramework client = createAndGetClient(clusterName);
        List<ZookeeperNode> nodes = new ArrayList<>();
        try {
            List<String> elements = zookeeper.getChildren(client, path, false);
            for (String element : elements) {
                String newPath = PathUtils.normalize_path(path + Cluster.ZK_SEPERATOR + element);
                ZookeeperNode node = new ZookeeperNode(path, element, hasChildren(client, newPath));
                nodes.add(node);
            }
        } catch (Exception e) {
            LOG.error("Get zookeeper info error!", e);
        }
        return nodes;
    }

    public static String getZKNodeData(String clusterName, String path) {
        CuratorFramework client = createAndGetClient(clusterName);
        String out = null;
        try {
            byte[] data = zookeeper.getData(client, PathUtils.normalize_path(path), false);
            if (data != null && data.length > 0) {
                Object obj = Utils.maybe_deserialize(data);
                out = gson.toJson(obj);
            }
        } catch (Exception e) {
            LOG.error("Get zookeeper data error!", e);
        }
        return out;
    }

    private static CuratorFramework createAndGetClient(String clusterName) {
        Map conf = Utils.readStormConfig();
        CuratorFramework client = zkClients.get(clusterName);
        if (client == null) {
            ClusterConfig config = UIUtils.clusterConfig.get(clusterName);
            List<String> zkServers = config.getZkServers();
            String zkRoot = config.getZkRoot();
            Integer zkPort = config.getZkPort();

            client = zookeeper.mkClient(conf, zkServers, zkPort, zkRoot);
            CuratorFramework old = zkClients.putIfAbsent(clusterName, client);
            if (old != null) {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
                client = old;
            }
        }
        return client;
    }

    public static boolean hasChildren(CuratorFramework client, String path) throws Exception {
        List<String> children = getChildren(client, path);
        return children != null && !children.isEmpty();
    }

    public static List<String> getChildren(CuratorFramework client, String path) throws Exception {
        return zookeeper.getChildren(client, path, false);
    }

    public static byte[] getData(CuratorFramework client, String path) throws Exception {
        return zookeeper.getData(client, path, false);
    }
}
