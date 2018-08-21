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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestQuerySpout extends BaseRichSpout {
  
  private static final long serialVersionUID = 5612868387548545552L;
  
  private BufferedReader br = null;
  SpoutOutputCollector collector = null;

  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    try {
      br = new BufferedReader(
          new FileReader(
              "src/test/resources/test.txt"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    this.collector = collector;
  }

  public void nextTuple() {
    try {
      String line = br.readLine();
      if (line == null) {
        return;
      }
      String[] dims = line.split("\t");
      collector.emit(new Values(dims[0]));
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id"));
  }

}
