package com.alibaba.jstorm.window;

import com.alibaba.jstorm.transactional.state.IRichCheckpointKvState;

public interface IRichCheckpointWindowedState<K, V, T> extends IRichCheckpointKvState<K, V, T>, IKvWindowedState<K, V> {
    
}