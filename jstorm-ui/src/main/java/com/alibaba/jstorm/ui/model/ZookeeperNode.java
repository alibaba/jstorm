package com.alibaba.jstorm.ui.model;

/**
 * @author Donne (lindeqiang1988@gmail.com)
 */
public class ZookeeperNode {
    private String pid;
    private String name;
    private String data;
    private boolean parent;

    public ZookeeperNode() {
    }

    public ZookeeperNode(String pid, String name, boolean parent) {
        super();
        this.pid = pid;
        this.name = name;
        this.parent = parent;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public boolean isParent() {
        return parent;
    }

    public void setParent(boolean parent) {
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return makeId(this.pid, this.name);
    }

    public String getPath() {
        String path = "";
        String id = pid + "/" + name;
        int pos = id.indexOf("/");
        if (pos != -1) {
            path = id.substring(pos);
        }
        return path;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public static String makeId(String pid, String name) {
        return pid + "/" + name;
    }
}
