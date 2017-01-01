package com.alibaba.jstorm.elasticsearch.mapper;

import org.elasticsearch.action.index.IndexRequest.OpType;

import backtype.storm.tuple.ITuple;

public class EsDefaultIndexMapper implements EsIndexMapper {

  private static final long serialVersionUID = 3777594656114668825L;

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
