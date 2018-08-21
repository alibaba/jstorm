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
package com.alibaba.jstorm.elasticsearch.userdefine;

import org.elasticsearch.action.index.IndexResponse;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.elasticsearch.bolt.EsAbstractBolt;
import com.alibaba.jstorm.elasticsearch.common.EsConfig;
import com.google.gson.JsonObject;

public class TestIndexBolt extends EsAbstractBolt {

  private static final long serialVersionUID = 8129061227572924508L;

  public TestIndexBolt(EsConfig esConfig) {
    super(esConfig);
  }

  @Override
  public void execute(Tuple tuple) {
    String line = tuple.getString(0);
    String[] dims = line.split("\t");
    JsonObject object = new JsonObject();
    object.addProperty("no", dims[1]);
    object.addProperty("date", dims[2]);
    IndexResponse indexResponse = client.prepareIndex("test", "test")
        .setId(dims[0]).setSource(object.toString()).execute().actionGet();
    collector.ack(tuple);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
