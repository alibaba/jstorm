package com.alibaba.jstorm.elasticsearch.bolt;

import org.elasticsearch.action.get.GetResponse;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.alibaba.jstorm.elasticsearch.common.EsOutputDeclarer;
import com.alibaba.jstorm.elasticsearch.mapper.EsQueryMapper;
import com.google.common.base.Preconditions;

public class EsQueryBolt extends EsAbstractBolt {

  private static final long serialVersionUID = -6500107598987890882L;
  
  private EsOutputDeclarer esOutputDeclarer;
  private EsQueryMapper mapper;

  public EsQueryBolt(EsConfig esConfig, EsQueryMapper mapper,
      EsOutputDeclarer esOutputDeclarer) {
    super(esConfig);
    this.esOutputDeclarer = esOutputDeclarer;
    this.mapper = mapper;
    Preconditions.checkArgument(esOutputDeclarer.getFields().length != 0,
        "EsOutputDeclarer should contain fields");
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      String index = mapper.getIndex(tuple);
      String type = mapper.getType(tuple);
      String id = mapper.getId(tuple);
      GetResponse response = client.prepareGet(index, type, id).execute()
          .actionGet();
      collector.emit(esOutputDeclarer.getValues(response.getSource()));
      collector.ack(tuple);
    } catch (Exception e) {
      collector.fail(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(esOutputDeclarer.getFields()));
  }

}
