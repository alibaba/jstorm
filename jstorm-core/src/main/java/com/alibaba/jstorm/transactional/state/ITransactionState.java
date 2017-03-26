package com.alibaba.jstorm.transactional.state;

import java.io.Serializable;

public interface ITransactionState extends Serializable {
    public void commit(TransactionState state);

    public TransactionState retrieveCheckpoint();
}