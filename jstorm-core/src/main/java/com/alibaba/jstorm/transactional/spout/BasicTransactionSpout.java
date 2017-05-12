package com.alibaba.jstorm.transactional.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

public class BasicTransactionSpout extends TransactionSpout {
    public BasicTransactionSpout(ITransactionSpoutExecutor spoutExecutor) {
        super(spoutExecutor);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        /**
         *  For backward compatibility to release 2.2.*, the conversion of output collector to 
         *  BasicSpoutOutputCollector is still kept here.
         */
        if (spoutExecutor instanceof BasicTransactionSpoutExecutor) {
            ((BasicTransactionSpoutExecutor) spoutExecutor).open(conf, context, new BasicSpoutOutputCollector(outputCollector));
        } else if (spoutExecutor instanceof IBasicTransactionSpoutExecutor) {
            ((IBasicTransactionSpoutExecutor) spoutExecutor).open(conf, context, new BasicSpoutOutputCollector(outputCollector));
        } else {
            spoutExecutor.open(conf, context, new SpoutOutputCollector(outputCollector));
        }
    }
}