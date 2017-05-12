package com.alibaba.jstorm.transactional.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.transactional.state.IKvState;
import com.alibaba.jstorm.transactional.state.TransactionStateOperator;

public abstract class DefaultStatefulBoltExecutor<K, V> extends TransactionStateOperator<K, V> implements ITransactionStatefulBoltExecutor {
    private static final long serialVersionUID = 1638411046827664146L;

    private IKvState<K, V> state;

    public abstract void prepare(Map stormConf, TopologyContext context, OutputCollector collector, IKvState<K, V> state);

    public abstract void execute(Tuple input, IKvState<K, V> state);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.state = createState(context);
        prepare(stormConf, context, collector, state);
    }

    @Override
    public void execute(Tuple input) {
        execute(input, state);
    }
    
}