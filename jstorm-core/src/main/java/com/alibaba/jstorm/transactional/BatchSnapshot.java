package com.alibaba.jstorm.transactional;

import java.io.Serializable;
import java.util.Map;

public class BatchSnapshot implements Serializable {
    private static final long serialVersionUID = -8237016732140138121L;

    private BatchGroupId batchGroupId = new BatchGroupId();
    // count of the tuples sent to target task
    private int tupleCount;

    public BatchSnapshot() {
        
    }

    public BatchSnapshot(BatchGroupId id, int tupleCount) {
        this.batchGroupId.setBatchGroupId(id);
        this.tupleCount = tupleCount;
    }

    public BatchGroupId getBatchGroupId() {
        return batchGroupId;
    }

    public void setBatchGroupId(BatchGroupId id) {
        this.batchGroupId.setBatchGroupId(id);
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public void setTupleCount(int tupleCount) {
        this.tupleCount = tupleCount;
    }

    @Override
    public String toString() {
        return "[" + batchGroupId + ", tupleCount:" + tupleCount + "]";
    }
}