package com.alibaba.jstorm.window;

import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.transactional.state.ITransactionStateOperator;

public abstract class TransactionalBaseWindowBolt<T extends Tuple, K, V> extends BaseWindowedBolt<T> implements ITransactionStateOperator {
    
}