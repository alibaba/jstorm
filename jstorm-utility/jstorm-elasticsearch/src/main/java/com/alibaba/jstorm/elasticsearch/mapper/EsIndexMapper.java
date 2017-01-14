package com.alibaba.jstorm.elasticsearch.mapper;

import org.elasticsearch.action.index.IndexRequest.OpType;

import backtype.storm.tuple.ITuple;

public interface EsIndexMapper extends EsMapper {

  public OpType getOpType();

  public String getId(ITuple tuple);

  public String getSource(ITuple tuple);

}
