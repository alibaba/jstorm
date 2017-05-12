package com.alibaba.jstorm.transactional;

public class DefaultCacheOperator implements ICacheOperator {

    @Override
    public PendingBatch createPendingBatch(long batchId) {
        return new PendingBatch(batchId);
    }
}