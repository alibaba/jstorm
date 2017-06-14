/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
