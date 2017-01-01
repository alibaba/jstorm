package com.alibaba.jstorm.elasticsearch.query;

import backtype.storm.tuple.ITuple;

import com.alibaba.jstorm.elasticsearch.mapper.EsQueryMapper;

public class TestQueryMapper implements EsQueryMapper {

  private static final long serialVersionUID = 9002508055282433046L;

  public String getIndex(ITuple tuple) {
    return "test";
  }

  public String getType(ITuple tuple) {
    return "test";
  }

  public String getId(ITuple tuple) {
    return tuple.getStringByField("id");
  }

}
