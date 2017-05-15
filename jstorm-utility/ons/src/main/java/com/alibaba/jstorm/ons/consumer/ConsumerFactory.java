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
package com.alibaba.jstorm.ons.consumer;

import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerFactory {
    private static final Logger LOG = Logger.getLogger(ConsumerFactory.class);

    public static Map<String, Consumer> consumers = new HashMap<String, Consumer>();

    public static synchronized Consumer mkInstance(ConsumerConfig consumerConfig, MessageListener listener) throws Exception {
        String consumerId = consumerConfig.getConsumerId();
        Consumer consumer = consumers.get(consumerId);
        if (consumer != null) {

            LOG.info("Consumer of " + consumerId + " has been created, don't recreate it ");

            // Attention, this place return null to info duplicated consumer
            return null;
        }

        Properties properties = new Properties();
        properties.put(PropertyKeyConst.AccessKey, consumerConfig.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, consumerConfig.getSecretKey());
        properties.put(PropertyKeyConst.ConsumerId, consumerId);
        properties.put(PropertyKeyConst.ConsumeThreadNums, consumerConfig.getConsumerThreadNum());
        consumer = ONSFactory.createConsumer(properties);

        consumer.subscribe(consumerConfig.getTopic(), consumerConfig.getSubExpress(), listener);
        consumer.start();

        consumers.put(consumerId, consumer);
        LOG.info("Successfully create " + consumerId + " consumer");

        return consumer;

    }

}
