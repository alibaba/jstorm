package com.alibaba.jstorm.transactional.state;

import java.util.Map;
import java.util.Set;

public interface ITopologyStateInitOperator {
    /**
     * @return userState
     */
    public Object getInitSpoutUserState(int currTaskId, Set<Integer> currSpoutTasks, Map<Integer, TransactionState> prevSpoutStates);

    /**
     * @return userState
     */
    public Object getInitBoltUserState(int currTaskId, Set<Integer> currBoltTasks, Map<Integer, TransactionState> prevBoltStates);

    /**
     * @return systemState
     */
    public Object getInitSpoutSysState(int currTaskId, Set<Integer> currSpoutTasks, Map<Integer, TransactionState> prevSpoutStates);

    /**
     * @return systemState
     */
    public Object getInitBoltSysState(int currTaskId, Set<Integer> currBoltTasks, Map<Integer, TransactionState> prevBoltStates);
}