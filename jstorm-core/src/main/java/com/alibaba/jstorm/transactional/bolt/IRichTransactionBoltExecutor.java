package com.alibaba.jstorm.transactional.bolt;

public interface IRichTransactionBoltExecutor extends ITransactionBoltExecutor {
    void finishBatch(long batchId);
}