package com.alibaba.jstorm.transactional.state;

import java.util.Map;
import java.util.Set;

public abstract class TopologyUserStateOperator implements IRichTopologyStateOperator {
    @Override
    public Object getInitSpoutSysState(int currTaskId, Set<Integer> currSpoutTasks, Map<Integer, TransactionState> prevSpoutStates) {
        Object ret = null;
        if (prevSpoutStates != null) {
            TransactionState state = prevSpoutStates.get(currTaskId);
            ret = state != null ? state.getsysCheckpoint() : null;
        }
        return ret;
    }

    @Override
    public Object getInitBoltSysState(int currTaskId, Set<Integer> currBoltTasks, Map<Integer, TransactionState> prevBoltStates) {
        Object ret = null;
        if (prevBoltStates != null) {
            TransactionState state = prevBoltStates.get(currTaskId);
            ret = state != null ? state.getsysCheckpoint() : null;
        }
        return ret;
    }
}