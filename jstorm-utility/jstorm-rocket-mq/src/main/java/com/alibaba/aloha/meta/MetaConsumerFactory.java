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
package com.alibaba.aloha.meta;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.taobao.metaq.client.MetaHelper;
import com.taobao.metaq.client.MetaPushConsumer;

public class MetaConsumerFactory {
	
	private static final Logger	LOG   = Logger.getLogger(MetaConsumerFactory.class);
	
    
    private static final long                 serialVersionUID = 4641537253577312163L;
    
    public static Map<String, MetaPushConsumer> consumers = 
    		new HashMap<String, MetaPushConsumer>();
    
    public static synchronized MetaPushConsumer mkInstance(MetaClientConfig config, 
			MessageListenerConcurrently listener)  throws Exception{
    	
    	String topic = config.getTopic();
    	String groupId = config.getConsumerGroup();
    	
    	String key = groupId;
    	
    	MetaPushConsumer consumer = consumers.get(key);
    	if (consumer != null) {
    		
    		LOG.info("Consumer of " + key + " has been created, don't recreate it ");
    		
    		//Attention, this place return null to info duplicated consumer
    		return null;
    	}
    	
        
        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init meta client \n");
        sb.append(",configuration:").append(config);
        
        LOG.info(sb.toString());
        
        consumer = new MetaPushConsumer(groupId);
        
        String nameServer = config.getNameServer();
        if ( nameServer != null) {
			String namekey = "rocketmq.namesrv.domain";

			String value = System.getProperty(namekey);
			// this is for alipay
			if (value == null) {

				System.setProperty(namekey, nameServer);
			} else if (value.equals(nameServer) == false) {
				throw new Exception(
						"Different nameserver address in the same worker "
								+ value + ":" + nameServer);

			}
		}
        
        String instanceName = groupId +"@" +	JStormUtils.process_pid();
		//consumer.setInstanceName(instanceName);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		consumer.subscribe(config.getTopic(), config.getSubExpress());
		consumer.registerMessageListener(listener);
		
		consumer.setPullThresholdForQueue(config.getQueueSize());
		consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
		consumer.setPullBatchSize(config.getPullBatchSize());
		consumer.setPullInterval(config.getPullInterval());
		consumer.setConsumeThreadMin(config.getPullThreadNum());
		consumer.setConsumeThreadMax(config.getPullThreadNum());


		Date date = config.getStartTimeStamp() ;
		if ( date != null) {
			LOG.info("Begin to reset meta offset to " + date);
			try {
				MetaHelper.resetOffsetByTimestamp(MessageModel.CLUSTERING,
					instanceName, config.getConsumerGroup(),
					config.getTopic(), date.getTime());
				LOG.info("Successfully reset meta offset to " + date);
			}catch(Exception e) {
				LOG.error("Failed to reset meta offset to " + date);
			}

		}else {
			LOG.info("Don't reset meta offset  ");
		}

		consumer.start();
		
		consumers.put(key, consumer);
		LOG.info("Successfully create " + key + " consumer");
		
		
		return consumer;
		
    }
    
}
