package com.alibaba.jstorm.transactional.state;

import java.io.Serializable;

import com.alibaba.jstorm.transactional.BatchGroupId;

public class TransactionState implements Serializable {
    private static final long serialVersionUID = 1124196216381387618L;

    public static enum State {
        INIT, ACTIVE, INACTIVE, ROLLBACK
    }

    protected BatchGroupId batchGroupId;
    protected Object systemCheckpoint;
    protected Object userCheckpoint;

    public TransactionState() {
    	
    }

    public TransactionState(int groupId, long batchId, Object sysCheckpoint, Object userCheckpoint) {
        this.batchGroupId = new BatchGroupId(groupId, batchId);
        this.systemCheckpoint = sysCheckpoint;
        this.userCheckpoint = userCheckpoint;
    }

    public TransactionState(TransactionState state) {
        this.batchGroupId = new BatchGroupId(state.getCurrBatchGroupId());
        this.systemCheckpoint = state.getsysCheckpoint();
        this.userCheckpoint = state.getUserCheckpoint();
    }

    public void setCurrBatchGroupId(BatchGroupId id) {
        this.batchGroupId = id;
    }

    public void setGroupId(int groupId) {
    	this.batchGroupId.groupId = groupId;
    }

    public void setBatchId(long batchId) {
    	this.batchGroupId.batchId = batchId;
    }

    public BatchGroupId getCurrBatchGroupId() {
        return batchGroupId;
    }

    public Object getsysCheckpoint() {
    	return systemCheckpoint;
    }

    public void setSystemCheckpoint(Object checkpoint) {
        systemCheckpoint = checkpoint;
    }

    public Object getUserCheckpoint() {
        return userCheckpoint;
    }

    public void setUserCheckpoint(Object checkpoint) {
        userCheckpoint = checkpoint;
    }

    public void reset() {
        batchGroupId = null;
        systemCheckpoint = null;
        userCheckpoint = null;
    }

    @Override
    public String toString() {
        return "batchGroupId=" + batchGroupId + ", sysState=" + systemCheckpoint + ", userState=" + userCheckpoint;
    }
}