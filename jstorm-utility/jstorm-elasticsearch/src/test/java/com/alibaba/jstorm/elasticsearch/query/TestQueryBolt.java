package com.alibaba.jstorm.elasticsearch.query;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TestQueryBolt extends BaseRichBolt {

  private static final long serialVersionUID = -4561790175801815097L;
  
  protected OutputCollector collector;
  FileOutputStream fos;

  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    collector = this.collector;
    try {
      fos = new FileOutputStream(
          "src/test/resources/test-query.txt");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void execute(Tuple input) {
    String date = input.getStringByField("date");
    try {
      fos.write((date + "\n").getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

}
