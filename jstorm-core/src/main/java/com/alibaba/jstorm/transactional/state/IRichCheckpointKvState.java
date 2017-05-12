package com.alibaba.jstorm.transactional.state;

public interface IRichCheckpointKvState<K, V, T> extends ICheckpointKvState<K, V, T> {
    public void setStateName(String stateName);
}