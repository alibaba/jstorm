package com.alibaba.jstorm.transactional.state;

public interface ICheckpointKvState<K, V, T> extends ICheckpoint<T>, IKvState<K, V> {
    
}