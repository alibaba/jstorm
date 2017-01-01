package com.alibaba.jstorm.elasticsearch.mapper;

import backtype.storm.tuple.ITuple;

public interface EsQueryMapper extends EsMapper {

  public String getId(ITuple tuple);
  
}
