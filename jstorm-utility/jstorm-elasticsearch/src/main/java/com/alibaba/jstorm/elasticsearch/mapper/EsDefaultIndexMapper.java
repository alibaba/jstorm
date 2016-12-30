package com.alibaba.jstorm.elasticsearch.mapper;

import org.elasticsearch.action.index.IndexRequest.OpType;

import backtype.storm.tuple.ITuple;

public class EsDefaultIndexMapper implements EsIndexMapper {

  @Override
  public OpType getOpType() {
    return OpType.INDEX;
  }

  @Override
  public String getIndex(ITuple tuple) {
    return tuple.getStringByField("index");
  }

  @Override
  public String getType(ITuple tuple) {
    return tuple.getStringByField("type");
  }

  @Override
  public String getId(ITuple tuple) {
    return tuple.getStringByField("id");
  }

  @Override
  public String getSource(ITuple tuple) {
    return tuple.getStringByField("source");
  }

}
