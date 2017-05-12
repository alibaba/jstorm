package com.alibaba.jstorm.transactional.state;

import java.io.Serializable;

import backtype.storm.task.TopologyContext;

public interface IState extends Serializable {
    /**
     * Initialization of state
     * @param context topology context
     */     
    public void init(TopologyContext context);

    /**
     * State cleanup
     */
    public void cleanup();
}