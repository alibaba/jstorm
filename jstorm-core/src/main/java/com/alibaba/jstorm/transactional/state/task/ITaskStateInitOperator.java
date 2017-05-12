package com.alibaba.jstorm.transactional.state.task;

import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.transactional.state.TransactionState;

public interface ITaskStateInitOperator {
    /**
     * 
     * @param conf Topoloyg config
     * @param currTaskId Id of the task that is going to be init
     * @param currComponentTasks All current tasks which belong to this component 
     * @param prevComponentTaskStates States of the previous tasks which belong to this component
     * @return task state
     */
    public Object getTaskInitState(Map conf, int currTaskId, Set<Integer> currComponentTasks, Map<Integer, TransactionState> prevComponentTaskStates);
}