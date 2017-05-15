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
package com.alibaba.jstorm.utils;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;

import backtype.storm.state.Serializer;
import backtype.storm.utils.Utils;

public class SerializerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SerializerFactory.class);

    public static <T> Serializer<T> createSerailzer(Map conf) {
        String serializerType = Utils.getString(conf.get(ConfigExtension.TOPOLOGY_SERIALIZER_TYPE), "kryo");
        if (serializerType.equals("java")) {
            return new JavaSerializer<T>();
        } else if (serializerType.equals("kryo")) {
            return new KryoSerializer<T>();
        } else {
            LOG.error("Unknown serializer type: {}", serializerType);
            return null;
        }
    }

    private SerializerFactory() {}
}