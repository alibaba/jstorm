package com.alipay.dw.jstorm.example.tm;

import java.util.Map;

import com.alibaba.jstorm.client.ConfigExtension;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class TMUdfStreamTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Map config = new Config();
        config.put(ConfigExtension.TOPOLOGY_MASTER_USER_DEFINED_STREAM_CLASS, "com.alipay.dw.jstorm.example.tm.TMUdfHandler");
        config.put(Config.TOPOLOGY_WORKERS, 2);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TMUdfSpout", new TMUdfSpout(), 2);
        builder.setBolt("TMUdfBolt", new TMUdfBolt(), 4);
        StormTopology topology = builder.createTopology();

        StormSubmitter.submitTopology("TMUdfTopology", config, topology);
    }
}