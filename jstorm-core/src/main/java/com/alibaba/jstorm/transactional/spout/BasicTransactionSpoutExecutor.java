package com.alibaba.jstorm.transactional.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

public abstract class BasicTransactionSpoutExecutor implements IBasicTransactionSpoutExecutor {
    private static final long serialVersionUID = 7078471332573649150L;

    public abstract void open(Map conf, TopologyContext context, BasicSpoutOutputCollector collector);

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        
    }
}