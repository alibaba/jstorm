package com.alipay.dw.jstorm.example.tm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.task.master.TopologyMaster;
import com.alipay.dw.jstorm.example.tm.TMUdfHandler.TMUdfMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TMUdfBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(TMUdfBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        
    }

    @Override
    public void execute(Tuple input) {
        Object value = input.getValue(0);
        if (input.getSourceStreamId().equals(TopologyMaster.USER_DEFINED_STREAM)) {
            TMUdfMessage message = (TMUdfMessage) value;
            LOG.info("Received TM UDF message trigged by task-{}", message.spoutTaskId);
        } else {
            LOG.info("Received unkown message: {}", input);
        }
    }

    @Override
    public void cleanup() {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
}