package com.jstorm.example.unittests.tick;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/21.
 */
public class TickTupleTestSpout implements IRichSpout{
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }
}
