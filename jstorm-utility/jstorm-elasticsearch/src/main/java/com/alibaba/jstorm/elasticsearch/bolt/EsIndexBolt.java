package com.alibaba.jstorm.elasticsearch.bolt;

import org.elasticsearch.action.index.IndexRequest.OpType;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.alibaba.jstorm.elasticsearch.mapper.EsIndexMapper;

public class EsIndexBolt extends EsAbstractBolt {

  private static final long serialVersionUID = 8177473361305606986L;

  private EsIndexMapper mapper;

  public EsIndexBolt(EsConfig esConfig, EsIndexMapper mapper) {
    super(esConfig);
    this.mapper = mapper;
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      String index = mapper.getIndex(tuple);
      String type = mapper.getType(tuple);
      String id = mapper.getId(tuple);
      String source = mapper.getSource(tuple);
      OpType opType = mapper.getOpType();
      client.prepareIndex(index, type).setId(id).setSource(source)
          .setOpType(opType).execute();
      collector.ack(tuple);
    } catch (Exception e) {
      collector.fail(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
