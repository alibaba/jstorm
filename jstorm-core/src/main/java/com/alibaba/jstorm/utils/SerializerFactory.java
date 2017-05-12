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