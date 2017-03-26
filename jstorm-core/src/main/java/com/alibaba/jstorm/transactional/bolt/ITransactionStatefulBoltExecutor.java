package com.alibaba.jstorm.transactional.bolt;

import com.alibaba.jstorm.transactional.state.ITransactionStateOperator;

public interface ITransactionStatefulBoltExecutor extends ITransactionBoltExecutor, ITransactionStateOperator {

}