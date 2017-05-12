package com.alibaba.jstorm.transactional.state;

import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;

public class DefaultTopologyStateOperator implements IRichTopologyStateOperator {
    private Map conf;

    @Override
    public Object initState(String topologyName) {
        return null;
    }

    @Override
    public boolean commitState(String topologyName, Object state) {
        return true;
    }

    @Override
    public void init(TopologyContext context) {
        this.conf = context.getStormConf();
    }

    @Override
    public Object getInitSpoutUserState(int currTaskId, Set<Integer> currSpoutTasks, Map<Integer, TransactionState> prevSpoutStates) {
        Object ret = null;
        if (prevSpoutStates != null) {
            TransactionState state = prevSpoutStates.get(currTaskId);
            ret = state != null ? state.getUserCheckpoint() : null;
        }
        return ret;
    }

    @Override
    public Object getInitBoltUserState(int currTaskId, Set<Integer> currBoltTasks, Map<Integer, TransactionState> prevBoltStates) {
        Object ret = null;
        if (prevBoltStates != null) {
            TransactionState state = prevBoltStates.get(currTaskId);
            ret = state != null ? state.getUserCheckpoint() : null;
        }
        return ret;
    }

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