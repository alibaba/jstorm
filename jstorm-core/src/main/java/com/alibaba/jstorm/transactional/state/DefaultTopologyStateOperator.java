package com.alibaba.jstorm.transactional.state;

import java.util.Map;

public class DefaultTopologyStateOperator implements ITopologyStateOperator {

    @Override
    public Object initState(String topologyName) {
        return null;
    }

    @Override
    public boolean commitState(String topologyName, int groupId, Object state) {
        return true;
    }

    @Override
    public void init(Map conf) {
        
    }
    
}