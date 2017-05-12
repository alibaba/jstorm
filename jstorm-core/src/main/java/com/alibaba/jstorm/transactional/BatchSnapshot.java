package com.alibaba.jstorm.transactional;

import java.io.Serializable;

public class BatchSnapshot implements Serializable {
    private static final long serialVersionUID = -8237016732140138121L;

    private long batchId;
    // count of the tuples sent to target task
    private int tupleCount;

    public BatchSnapshot() {
    }

    public BatchSnapshot(long batchId, int tupleCount) {
        this.batchId = batchId;
        this.tupleCount = tupleCount;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long id) {
        this.batchId = id;
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public void setTupleCount(int tupleCount) {
        this.tupleCount = tupleCount;
    }

    @Override
    public String toString() {
        return "[batchId-" + batchId + ", tupleCount:" + tupleCount + "]";
    }
}