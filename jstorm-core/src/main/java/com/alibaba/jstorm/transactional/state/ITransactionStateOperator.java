package com.alibaba.jstorm.transactional.state;

import java.io.Serializable;

import com.alibaba.jstorm.transactional.BatchGroupId;

public interface ITransactionStateOperator extends Serializable {
    /**
     * 
     */
    public void initState(Object userState);

    /**
     * Called when current batch is finished
     * @return user state to be committed
     */
    public Object finishBatch();

    /**
     * 
     * @param The user state Data
     * @return snapshot state which is used to retrieve the persistent user state
     */
    public Object commit(BatchGroupId id, Object state);

    /**
     * 
     * @param user state for rollback
     */
    public void rollBack(Object userState);

    /**
     * Called when the whole topology finishes committing
     */
    public void ackCommit(BatchGroupId id);
}