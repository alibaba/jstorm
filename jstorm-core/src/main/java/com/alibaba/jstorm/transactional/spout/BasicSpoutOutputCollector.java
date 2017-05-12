package com.alibaba.jstorm.transactional.spout;

import backtype.storm.spout.SpoutOutputCollector;

public class BasicSpoutOutputCollector extends SpoutOutputCollector {
    protected TransactionSpoutOutputCollector collector;
    
    public BasicSpoutOutputCollector(TransactionSpoutOutputCollector delegate) {
        super(delegate);
        this.collector = delegate;
    }

    public void emitBarrier() {
        collector.emitBarrier();
    }
}