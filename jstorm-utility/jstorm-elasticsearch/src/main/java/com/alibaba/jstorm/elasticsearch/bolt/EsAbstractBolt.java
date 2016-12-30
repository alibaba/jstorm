package com.alibaba.jstorm.elasticsearch.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.alibaba.jstorm.elasticsearch.common.StormEsClient;
import com.google.common.base.Preconditions;

/*
 * Author(s): Ray on 2016/12/29
 */
public abstract class EsAbstractBolt extends BaseRichBolt {

  private static final long serialVersionUID = 1L;

  private static final Logger logger = Logger.getLogger(EsAbstractBolt.class);

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
      logger.warn("unable to initialize ESBolt ", e);
    }
  }

  @Override
  public abstract void execute(Tuple tuple);

  static Client getClient() {
    return EsAbstractBolt.client;
  }

}
