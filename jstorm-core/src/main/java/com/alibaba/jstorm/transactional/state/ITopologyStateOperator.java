package com.alibaba.jstorm.transactional.state;

import backtype.storm.task.TopologyContext;

public interface ITopologyStateOperator {
    void init(TopologyContext context);

    Object initState(String topologyName);

    boolean commitState(String topologyName, Object state);
}