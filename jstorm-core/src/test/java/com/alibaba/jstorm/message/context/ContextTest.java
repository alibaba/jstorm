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
package com.alibaba.jstorm.message.context;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Maps;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;

public class ContextTest {
    @Test
    public void test_netty() {
        Map storm_conf = Maps.newHashMap();
        storm_conf.put(Config.STORM_MESSAGING_TRANSPORT, "com.alibaba.jstorm.message.netty.NettyContext");
        storm_conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
        storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
        storm_conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
        storm_conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
        storm_conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
        storm_conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
        IContext context = TransportFactory.makeContext(storm_conf);
        Assert.assertNotNull(context);
    }
}
