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
package com.alibaba.jstorm.batch.example;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.batch.BatchId;

public class TransformBolt implements IBasicBolt {
	private static final long serialVersionUID = 5720810158625748044L;

	private static final Logger LOG = Logger.getLogger(TransformBolt.class);

	public static final String BOLT_NAME = TransformBolt.class.getSimpleName();

	private Map conf;

	private Random rand;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.conf = stormConf;
		this.rand = new Random();
		rand.setSeed(System.currentTimeMillis());
		
		LOG.info("Successfully do prepare");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		BatchId id = (BatchId) input.getValue(0);

		long value = rand.nextInt(100);

		collector.emit(new Values(id, value));

	}

	@Override
	public void cleanup() {
		LOG.info("Successfully do cleanup");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("BatchId", "Value"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
