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
package com.alibaba.jstorm.elasticsearch.query;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TestQueryBolt extends BaseRichBolt {

  private static final long serialVersionUID = -4561790175801815097L;
  
  protected OutputCollector collector;
  FileOutputStream fos;

  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    collector = this.collector;
    try {
      fos = new FileOutputStream(
          "src/test/resources/test-query.txt");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void execute(Tuple input) {
    String date = input.getStringByField("date");
    try {
      fos.write((date + "\n").getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

}
