package com.alibaba.jstorm.elasticsearch.bolt;

import java.util.Map;

import org.elasticsearch.client.Client;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.alibaba.jstorm.elasticsearch.common.StormEsClient;
import com.google.common.base.Preconditions;

public abstract class EsAbstractBolt extends BaseRichBolt {

  private static final long serialVersionUID = 6213594268206022374L;

  protected static Client client;

  protected OutputCollector collector;
  private EsConfig esConfig;

  public EsAbstractBolt(EsConfig esConfig) {
    Preconditions.checkNotNull(esConfig);
    this.esConfig = esConfig;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext,
      OutputCollector outputCollector) {
    try {
      this.collector = outputCollector;
      synchronized (EsAbstractBolt.class) {
        if (client == null) {
          client = new StormEsClient(esConfig);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("unable to initialize EsBolt ", e);
    }
  }

  @Override
  public abstract void execute(Tuple tuple);

  static Client getClient() {
    return EsAbstractBolt.client;
  }

}
