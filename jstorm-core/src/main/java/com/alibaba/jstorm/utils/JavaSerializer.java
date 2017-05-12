package com.alibaba.jstorm.utils;

import backtype.storm.state.Serializer;
import backtype.storm.utils.Utils;

public class JavaSerializer<T> implements Serializer<T> {
    private static final long serialVersionUID = -7722752318778774832L;

    public JavaSerializer() {
        
    }

    @Override
    public byte[] serialize(T obj) {
        if (obj != null)
            return Utils.serialize(obj);
        else
            return null;
    }

    @Override
    public T deserialize(byte[] b) {
        if (b != null)
            return (T) Utils.javaDeserialize(b);
        else
            return null;
    }
}