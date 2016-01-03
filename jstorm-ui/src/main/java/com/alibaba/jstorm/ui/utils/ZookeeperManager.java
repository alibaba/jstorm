package com.alibaba.jstorm.ui.utils;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.DistributedClusterState;
import com.alibaba.jstorm.ui.model.ZookeeperNode;
import com.alibaba.jstorm.utils.PathUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
    private static ConcurrentMap<String, ClusterState> clusterSates = new ConcurrentHashMap<>();
    private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static List<ZookeeperNode> listZKNodes(String clusterName, String parent) {
        List<ZookeeperNode> nodes = new ArrayList<>();
        try {
            ClusterState clusterState = getAndCreateClusterState(clusterName);
            if (clusterState == null) {
                throw new IllegalStateException("Cluster state is null");
            }

            List<String> elements = clusterState.get_children(parent, false);
            for (String element : elements) {
                String path = PathUtils.normalize_path(parent + Cluster.ZK_SEPERATOR + element);
                nodes.add(new ZookeeperNode(parent, element, hasChildren(clusterState, path)));
            }
        } catch (Exception e) {
            LOG.error("Get zookeeper info error!", e);
        }
        return nodes;
    }

    public static String getZKNodeData(String clusterName, String path) {
        String out = null;
        try {
            ClusterState clusterState = getAndCreateClusterState(clusterName);
            if (clusterState == null) {
                throw new IllegalStateException("Cluster state is null");
            }

            byte[] data = clusterState.get_data(PathUtils.normalize_path(path), false);
            if (data != null && data.length > 0) {
                Object obj = Utils.maybe_deserialize(data);
                if (obj != null){
                    out = gson.toJson(obj);
                } else {
                    out = new String(data);
                }
            }
        } catch (Exception e) {
            LOG.error("Get zookeeper data error!", e);
        }
        return out;
    }

    private static ClusterState getAndCreateClusterState(String clusterName) {
        ClusterState state = null;
        try {
            state = clusterSates.get(clusterName);
            if (state == null) {
                Map zkConf = UIUtils.resetZKConfig(Utils.readStormConfig(), clusterName);
                state = new DistributedClusterState(zkConf);
                ClusterState old = clusterSates.putIfAbsent(clusterName, state);
                if (old != null) {
                    try {
                        state.close();
                    } catch (Exception e) {
                        LOG.warn("Close state error!", e);
                    }
                    state = old;
                }
            }
        } catch (Exception e) {
            LOG.error("Create cluster state error!");
        }

        return state;
    }

    public static boolean hasChildren(ClusterState clusterState, String path) throws Exception {
        if (clusterState == null || path == null) {
            return false;
        }

        List<String> children = clusterState.get_children(path, false);
        return children != null && !children.isEmpty();
    }
}
