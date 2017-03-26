package com.alibaba.jstorm.transactional.spout;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

public class BasicTransactionSpout extends TransactionSpout {
    private ScheduledExecutorService scheduledService = null;

	public BasicTransactionSpout(ITransactionSpoutExecutor spoutExecutor) {
		super(spoutExecutor);
	}

	@Override 
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        int threadPoolNum = JStormUtils.parseInt(conf.get("transaction.schedule.thread.pool"), 1);
        int delay = JStormUtils.parseInt(conf.get("transaction.schedule.batch.delay.ms"), 1000);
        int initDelay = delay >= 30000 ? 30000 : delay;
        if (scheduledService == null) {
            scheduledService = Executors.newScheduledThreadPool(threadPoolNum);
        }
        scheduledService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                process(Operation.commit, null);
            } 
        }, initDelay, delay, TimeUnit.MILLISECONDS);
	}
}