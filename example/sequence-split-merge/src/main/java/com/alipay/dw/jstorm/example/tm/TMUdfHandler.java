package com.alipay.dw.jstorm.example.tm;

import java.util.Collection;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMaster;
import com.alibaba.jstorm.task.master.TopologyMasterContext;

public class TMUdfHandler implements TMHandler {
    public static class TMUdfMessage {
        public int spoutTaskId;

        public TMUdfMessage(int taskId) {
            this.spoutTaskId = taskId;
        }
    }

    Collection<Integer> boltTasks;
    OutputCollector collector;

    @Override
    public void init(TopologyMasterContext tmContext) {
        TopologyContext context = tmContext.getContext();
        boltTasks = context.getComponentTasks("TMUdfBolt");
        collector = tmContext.getCollector();
    }

    @Override
    public void process(Object event) throws Exception {
        Tuple tuple = (Tuple) event;
        for (int boltTaskId : boltTasks) {
            collector.emitDirectCtrl(boltTaskId, TopologyMaster.USER_DEFINED_STREAM, tuple.getValues());
        }
    }

    @Override
    public void cleanup() {
        
    }
    
}