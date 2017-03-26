package com.alibaba.jstorm.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.zk.Zookeeper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import backtype.storm.Config;
import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.utils.Utils;

/**
 * Created by dingjun on 15-12-17.
 */
public class ZooKeeperDataViewTest {

    private static Zookeeper zkobj;
    static CuratorFramework  zk;
    static Gson              gson;

    public static final String CONFIG_FILE = "/.jstorm/storm.yaml";
    public static boolean      SKIP        = false;

    @BeforeClass
    public static void init() {
        String CONFIG_PATH = System.getProperty("user.home") + CONFIG_FILE;
        File file = new File(CONFIG_PATH);
        if (file.exists() == false) {
            SKIP = true;
            return;
        }

        try {
            zkobj = new Zookeeper();
            System.getProperties().setProperty("storm.conf.file", CONFIG_PATH);
            Map conf = Utils.readStormConfig();
            zk = zkobj.mkClient(conf,
                    (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                    conf.get(Config.STORM_ZOOKEEPER_PORT),
                    (String) conf.get(Config.STORM_ZOOKEEPER_ROOT));
            gson = new GsonBuilder().setPrettyPrinting().create();
        }catch(Throwable e) {
            e.printStackTrace();
            SKIP = true;
        }
    }

    @Test
    public void viewAssignments() throws Exception {
        if (SKIP == true) {
            return;
        }
        List<String> assignments = zkobj.getChildren(zk,
                Cluster.ASSIGNMENTS_SUBTREE, false);
        for (String child : assignments) {
            byte[] data = zkobj.getData(zk, Cluster.assignment_path(child),
                    false);
            Assignment assignment = (Assignment) Utils.maybe_deserialize(data);
            System.out.println(gson.toJson(assignment));
        }
    }

    @Test
    public void viewTaskbeats() throws Exception {
        if (SKIP == true) {
            return;
        }
        List<String> assignments = zkobj.getChildren(zk,
                Cluster.TASKBEATS_SUBTREE, false);
        for (String child : assignments) {
            byte[] data = zkobj.getData(zk, Cluster.taskbeat_storm_root(child),
                    false);
            TopologyTaskHbInfo taskHbInfo = (TopologyTaskHbInfo) Utils
                    .maybe_deserialize(data);
            System.out.println(gson.toJson(taskHbInfo));
        }

    }

    @Test
    public void viewTopology() throws Exception {
        if (SKIP == true) {
            return;
        }
        List<String> assignments = zkobj.getChildren(zk, Cluster.STORMS_SUBTREE,
                false);
        for (String child : assignments) {
            byte[] data = zkobj.getData(zk, Cluster.storm_path(child), false);
            StormBase stormBase = (StormBase) Utils.maybe_deserialize(data);
            System.out.println(gson.toJson(stormBase));
        }
    }

    @Test
    public void viewMetrics() throws Exception {
        if (SKIP == true) {
            return;
        }

        List<String> assignments = zkobj.getChildren(zk, Cluster.METRIC_SUBTREE,
                false);
        for (String child : assignments) {
            byte[] data = zkobj.getData(zk, Cluster.metric_path(child), false);
            Integer size = (Integer) Utils.maybe_deserialize(data);
            System.out.println(size.toString());
        }
    }

    @Test
    public void viewTasks() throws Exception {
        if (SKIP == true) {
            return;
        }

        List<String> assignments = zkobj.getChildren(zk, Cluster.TASKS_SUBTREE,
                false);
        for (String child : assignments) {
            byte[] data = zkobj.getData(zk, Cluster.storm_task_root(child),
                    false);
            Map<Integer, TaskInfo> taskInfoMap = (Map<Integer, TaskInfo>) Utils
                    .maybe_deserialize(data);
            System.out.println(gson.toJson(taskInfoMap));
        }
    }

    @Test
    public void viewSupervisors() throws Exception {
        if (SKIP == true) {
            return;
        }

        List<String> assignments = zkobj.getChildren(zk,
                Cluster.SUPERVISORS_SUBTREE, false);
        for (String child : assignments) {
            byte[] data = zkobj.getData(zk, Cluster.supervisor_path(child),
                    false);
            SupervisorInfo supervisorInfo = (SupervisorInfo) Utils
                    .maybe_deserialize(data);
            System.out.println(gson.toJson(supervisorInfo));
        }
    }

    private void viewNode(Node parent) throws Exception {

        List<String> elements = zkobj.getChildren(zk, parent.getPath(), false);
        for (String element : elements) {
            Node node = new Node(
                    (parent.getPath().length() > 1 ? parent.getPath() : "")
                            + Cluster.ZK_SEPERATOR + element);
            byte[] data = zkobj.getData(zk, node.getPath(), false);
            if (data != null && data.length > 0) {
                Object obj = Utils.maybe_deserialize(data);
                node.setData(obj);
            }
            parent.addNode(node);
            viewNode(node);
        }
    }

    @Test
    public void viewAll() throws Exception {
        if (SKIP == true) {
            return;
        }

        Node root = new Node(Cluster.ZK_SEPERATOR);
        viewNode(root);

        System.out.println(gson.toJson(root));
    }

    public class Node {
        private String     path;
        private Object     data;
        private List<Node> children = new ArrayList<Node>();

        public Node() {

        }

        public Node(String path) {
            this.path = path;
        }

        public Node(String path, Object data) {
            this.path = path;
            this.data = data;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        public List<Node> getChildren() {
            return children;
        }

        public void setChildren(List<Node> children) {
            this.children = children;
        }

        public void addNode(Node node) {
            this.children.add(node);
        }
    }

    @AfterClass
    public static void close() {
        if (zk != null) {
            try {
                zk.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

}
