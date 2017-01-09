package com.alipay.dw.jstorm.example.userdefined.kryo;

import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
//import com.esotericsoftware.kryo.Kryo;
//import com.esotericsoftware.kryo.Serializer;
//import com.esotericsoftware.kryo.io.Input;
//import com.esotericsoftware.kryo.io.Output;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class PairSerializer extends Serializer<Pair> {
    
    @Override
    public Pair read(Kryo kryo, Input input, Class<Pair> arg2) {
        
        long value = input.readLong();
        String key = input.readString();
        Pair inner = new Pair();
        inner.setKey(key);
        inner.setValue(value);
        return inner;
    }
    
    @Override
    public void write(Kryo kryo, Output output, Pair inner) {
        output.writeLong(inner.getValue());
        output.writeString(inner.getKey());
    }
}
