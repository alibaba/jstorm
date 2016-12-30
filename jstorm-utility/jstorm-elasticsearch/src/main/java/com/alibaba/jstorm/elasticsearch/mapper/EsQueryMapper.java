package com.alibaba.jstorm.elasticsearch.mapper;

import backtype.storm.tuple.ITuple;

public interface EsQueryMapper extends EsMapper {

  public String getIndex(ITuple tuple);

  public String getType(ITuple tuple);

  public String getId(ITuple tuple);
}
