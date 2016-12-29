package com.alibaba.starter.utils;

public class KillAllTopology {

    public static void main(String[] args) {
        JStormHelper.cleanCluster();
    }
}
