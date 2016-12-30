package com.alibaba.jstorm.elasticsearch.query;

import backtype.storm.tuple.ITuple;

import com.alibaba.jstorm.elasticsearch.mapper.EsQueryMapper;

public class TestQueryMapper implements EsQueryMapper {

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
