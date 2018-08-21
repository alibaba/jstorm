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
package com.alibaba.jstorm.kafka;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.kafka.PartitionConsumer.EmitState;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class KafkaSpout implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

	protected SpoutOutputCollector collector;
	
	private long lastUpdateMs;
	PartitionCoordinator coordinator;
	
	private KafkaSpoutConfig config;
	
	private ZkState zkState;
	
	public KafkaSpout() {
	    
	}
	
	public KafkaSpout(KafkaSpoutConfig config) {
		this.config = config;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if (this.config == null) {
			config = new KafkaSpoutConfig();
		}
		config.configure(conf);
		zkState = new ZkState(conf, config);
		coordinator = new PartitionCoordinator(conf, config, context, zkState);
		lastUpdateMs = System.currentTimeMillis();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	    zkState.close();
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		Collection<PartitionConsumer> partitionConsumers = coordinator.getPartitionConsumers();
		for(PartitionConsumer consumer: partitionConsumers) {
			EmitState state = consumer.emit(collector);
			LOG.debug("====== partition "+ consumer.getPartition() + " emit message state is "+state);
//			if(state != EmitState.EMIT_MORE) {
//				currentPartitionIndex  = (currentPartitionIndex+1) % consumerSize;
//			}
//			if(state != EmitState.EMIT_NONE) {
//				break;
//			}
		}
		long now = System.currentTimeMillis();
        if((now - lastUpdateMs) > config.offsetUpdateIntervalMs) {
            commitState();
        }
        
		
	}
	
	public void commitState() {
	    lastUpdateMs = System.currentTimeMillis();
		for(PartitionConsumer consumer: coordinator.getPartitionConsumers()) {
			consumer.commitState();
        }
		
	}

	@Override
	public void ack(Object msgId) {
		KafkaMessageId messageId = (KafkaMessageId)msgId;
		PartitionConsumer consumer = coordinator.getConsumer(messageId.getPartition());
		consumer.ack(messageId.getOffset());
	}

	@Override
	public void fail(Object msgId) {
		KafkaMessageId messageId = (KafkaMessageId)msgId;
		PartitionConsumer consumer = coordinator.getConsumer(messageId.getPartition());
		consumer.fail(messageId.getOffset());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bytes"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	

}