package com.alibaba.jstorm.transactional.state;

import java.util.Map;

public interface ITopologyStateOperator {
    public void init(Map conf);

    public Object initState(String topologyName);

    public boolean commitState(String topologyName, int groupId, Object state);
}