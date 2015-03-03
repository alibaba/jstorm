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
package com.alibaba.jstorm.test.kafka;

import java.util.Properties;

import kafka.server.KafkaServerStartable;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;

public class KafkaTest {
    private final int port = 49123;
    private KafkaServerStartable kafka;
    private TestingServer server;
    private String zookeeperConnectionString;

    public KafkaTest() {}
    
    public void run() {
        try {
            server = new TestingServer();
            zookeeperConnectionString = server.getConnectString();
            ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(
                    1000, 3);
            CuratorFramework zookeeper = CuratorFrameworkFactory.newClient(
                    zookeeperConnectionString, retryPolicy);
            zookeeper.start();
            Properties p = new Properties();
            p.setProperty("zookeeper.connect", zookeeperConnectionString);
            p.setProperty("broker.id", "0");
            p.setProperty("port", "" + port);
            kafka.server.KafkaConfig config = new kafka.server.KafkaConfig(p);
            kafka = new KafkaServerStartable(config);
            kafka.startup();
        } catch (Exception ex) {
            throw new RuntimeException("Could not start test broker", ex);
        }
    }

    public String getBrokerConnectionString() {
        return "localhost:" + port;
    }

    public int getPort() {
        return port;
    }

    public void shutdown() {
        kafka.shutdown();
    }
    
    public static void main(String[] args) {
        KafkaTest test = new KafkaTest();
        test.run();
    }
}
