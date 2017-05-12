package com.alibaba.jstorm.transactional.state;

public interface ICheckpoint<T> {
    /**
     * restore state by the specified checkpoint
     * @param checkpoint
     */
    public void restore(T checkpoint);

    /**
     * backup state for a batch
     * @param batchId
     * @return
     */
    public T backup(long batchId);

    /**
     * checkpoint for a batch
     * @param batchId
     */
    public void checkpoint(long batchId);

    /**
     * remove the backup file both in local or remote storage for a successful batch
     * @param batchId successful batch id
     */
    public void remove(long batchId);
}