package com.alibaba.jstorm.elasticsearch.userdefine;

import org.elasticsearch.action.index.IndexResponse;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.elasticsearch.bolt.EsAbstractBolt;
import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.google.gson.JsonObject;

public class TestIndexBolt extends EsAbstractBolt {

  private static final long serialVersionUID = 8129061227572924508L;

  public TestIndexBolt(EsConfig esConfig) {
    super(esConfig);
  }

  @Override
  public void execute(Tuple tuple) {
    String line = tuple.getString(0);
    String[] dims = line.split("\t");
    JsonObject object = new JsonObject();
    object.addProperty("no", dims[1]);
    object.addProperty("date", dims[2]);
    IndexResponse indexResponse = client.prepareIndex("test", "test")
        .setId(dims[0]).setSource(object.toString()).execute().actionGet();
    collector.ack(tuple);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
