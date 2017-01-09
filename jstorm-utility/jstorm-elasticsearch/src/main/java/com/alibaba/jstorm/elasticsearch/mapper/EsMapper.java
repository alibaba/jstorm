package com.alibaba.jstorm.elasticsearch.mapper;

import java.io.Serializable;

import backtype.storm.tuple.ITuple;

public interface EsMapper extends Serializable {

  public String getIndex(ITuple tuple);

  public String getType(ITuple tuple);
}
