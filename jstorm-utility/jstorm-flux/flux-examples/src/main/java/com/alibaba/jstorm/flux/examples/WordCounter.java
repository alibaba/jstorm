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
package com.alibaba.jstorm.flux.examples;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;


/**
 * This bolt is used by the HBase example. It simply emits the first field
 * found in the incoming tuple as "word", with a "count" of `1`.
 *
 * In this case, the downstream HBase bolt handles the counting, so a value
 * of `1` will just increment the HBase counter by one.
 */
public class WordCounter extends BaseBasicBolt {
    public static Logger LOG = LoggerFactory.getLogger(WordCounter.class);

    Map<String, Integer> _counts;

    public void prepare(Map stormConf, TopologyContext context) {
        _counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValues().get(0);
        int count = 0;
        if (_counts.containsKey(word)) {
            count = _counts.get(word);
        }
        count++;
        _counts.put(word, count);
        collector.emit(tuple(word, count));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
