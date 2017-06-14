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
package com.alibaba.jstorm.ons.producer;

import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ProducerFactory {

    private static final Logger LOG = Logger.getLogger(ProducerFactory.class);

    public static Map<String, Producer> producers = new HashMap<String, Producer>();

    public static synchronized Producer mkInstance(ProducerConfig producerConfig) throws Exception {

        String producerId = producerConfig.getProducerId();
        Producer producer = producers.get(producerId);
        if (producer != null) {

            LOG.info("Producer of " + producerId + " has been created, don't recreate it ");
            return producer;
        }

        Properties properties = new Properties();
        properties.put(PropertyKeyConst.ProducerId, producerConfig.getProducerId());
        properties.put(PropertyKeyConst.AccessKey, producerConfig.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, producerConfig.getSecretKey());

        producer = ONSFactory.createProducer(properties);
        producer.start();


        producers.put(producerId, producer);
        LOG.info("Successfully create " + producerId + " producer");

        return producer;

    }

    public static synchronized void rmInstance(String producerId) {
        Producer producer = producers.remove(producerId);
        if (producer == null) {

            LOG.info("Producer of " + producerId + " has already been shutdown ");
            return;
        }

        producer.shutdown();
        LOG.info("Producer of " + producerId + " has been shutdown ");
    }

}
