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

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.jstorm.ons.OnsConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.aliyun.openservices.ons.api.PropertyKeyConst;

public class ConsumerConfig extends OnsConfig {

    private static final long serialVersionUID = 4292162795544528064L;
    private final String consumerId;
    private final int consumerThreadNum;
    private final String nameServer;

    public ConsumerConfig(Map conf) {
        super(conf);

        consumerId = (String) conf.get(PropertyKeyConst.ConsumerId);
        if (StringUtils.isBlank(consumerId)) {
            throw new RuntimeException(PropertyKeyConst.ConsumerId + " hasn't been set");
        }
        consumerThreadNum = JStormUtils.parseInt(
                conf.get(PropertyKeyConst.ConsumeThreadNums), 4);

        nameServer = (String) conf.get(PropertyKeyConst.NAMESRV_ADDR);
        if (nameServer != null) {
            String namekey = "rocketmq.namesrv.domain";

            String value = System.getProperty(namekey);
            if (value == null) {

                System.setProperty(namekey, nameServer);
            } else if (!value.equals(nameServer)) {
                throw new RuntimeException("Different nameserver address in the same worker "
                        + value + ":" + nameServer);
            }
        }
    }

    public String getConsumerId() {
        return consumerId;
    }

    public int getConsumerThreadNum() {
        return consumerThreadNum;
    }

    public String getNameServer() {
        return nameServer;
    }
}
