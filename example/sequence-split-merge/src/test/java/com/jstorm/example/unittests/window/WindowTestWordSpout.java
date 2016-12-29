package com.jstorm.example.unittests.window;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by binyang.dby on 2016/7/11.
 *
 * basically the same as TestWordSpout, just add some code to write the emit sequence to file for Assert.
 */
public class WindowTestWordSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
    private Random random = new Random();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        JStormUtils.sleepMs(100);
        String word = words[random.nextInt(words.length)];
        collector.emit(new Values(word));
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return conf;
    }
}
