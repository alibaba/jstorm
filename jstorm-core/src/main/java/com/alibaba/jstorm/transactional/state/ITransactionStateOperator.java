package com.alibaba.jstorm.transactional.state;

import java.io.Serializable;

public interface ITransactionStateOperator extends Serializable {
    void initState(Object userState);

    /**
     * Called when current batch is finished
     *
     * @return user state to be committed
     */
    Object finishBatch(long batchId);

    /**
     * @return snapshot state which is used to retrieve the persistent user state
     */
    Object commit(long batchId, Object state);

    /**
     * @param userState user state for rollback
     */
    void rollBack(Object userState);

    /**
     * Called when the whole topology finishes committing
     */
    void ackCommit(long batchId, long timeStamp);
}