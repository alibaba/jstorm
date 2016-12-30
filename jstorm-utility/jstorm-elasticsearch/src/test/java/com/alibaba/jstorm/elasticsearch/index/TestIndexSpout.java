package com.alibaba.jstorm.elasticsearch.index;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestIndexSpout extends BaseRichSpout{

  private BufferedReader br = null;
  SpoutOutputCollector collector = null;
  
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    try {
      br = new BufferedReader(new FileReader("src/test/resources/test.txt"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    this.collector = collector;
  }

  public void nextTuple() {
    try {
      String line = br.readLine();
      collector.emit(new Values(line));
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}
