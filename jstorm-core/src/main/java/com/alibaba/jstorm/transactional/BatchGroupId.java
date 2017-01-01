package com.alibaba.jstorm.transactional;

import java.io.Serializable;

public class BatchGroupId implements Serializable {
    private static final long serialVersionUID = 6836131129337696538L;

    public int groupId;
    public long batchId;

    public BatchGroupId() {

    }

    public BatchGroupId(int groupId, long batchId) {
        this.groupId = groupId;
        this.batchId = batchId;
    }

    public BatchGroupId(BatchGroupId batchGroupId) {
        setBatchGroupId(batchGroupId);
    }

    public void setBatchGroupId(BatchGroupId batchGroupId) {
        this.groupId = batchGroupId.groupId;
        this.batchId = batchGroupId.batchId;
    }

    public void reset() {
        groupId = 0;
        batchId = 0;
    }

    

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (batchId ^ (batchId >>> 32));
		result = prime * result + groupId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BatchGroupId other = (BatchGroupId) obj;
		if (batchId != other.batchId)
			return false;
		if (groupId != other.groupId)
			return false;
		return true;
	}

	@Override
    public String toString() {
        return "[groupId-" + groupId + ", batchId-" + batchId + "]";
    }
}