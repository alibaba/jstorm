package com.alibaba.jstorm.transactional;

public interface ICacheOperator {
    PendingBatch createPendingBatch(long batchId);
}