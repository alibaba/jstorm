package com.alibaba.jstorm.transactional.spout;

import com.alibaba.jstorm.transactional.state.ITransactionStateOperator;

import backtype.storm.topology.IRichSpout;

public interface ITransactionSpoutExecutor extends IRichSpout, ITransactionStateOperator {

}