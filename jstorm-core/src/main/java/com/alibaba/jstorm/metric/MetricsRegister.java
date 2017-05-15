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
package com.alibaba.jstorm.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.NimbusClientWrapper;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.1.2
 */
public class MetricsRegister {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private Map conf;
    private String topologyId;
    private NimbusClientWrapper client = null;
    private final Object lock = new Object();

    public MetricsRegister(Map conf, String topologyId) {
        this.conf = conf;
        this.topologyId = topologyId;
    }

    public Map<String, Long> registerMetrics(Set<String> names) {
        if (!JStormMetrics.enabled) {
            return new HashMap<>();
        }
        try {
            synchronized (lock){
                if (client == null) {
                    client = new NimbusClientWrapper();
                    client.init(conf);
                }
            }
            return client.getClient().registerMetrics(topologyId, names);
        } catch (Exception e) {
            LOG.error("Failed to gen metric ids", e);
            if (client != null) {
                client.cleanup();
                client = null;
            }
        }

        return new HashMap<>();
    }

}
