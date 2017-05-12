package com.alibaba.jstorm.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import backtype.storm.state.Serializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class KryoSerializer<T> implements Serializer<T> {
    private static final long serialVersionUID = 4529662972648507109L;

    private Kryo kryo;
    private Output output;
    private Input input;

    public KryoSerializer() {
        kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        output = new Output(200, 2000000000);
        input = new Input(1);
    }

    @Override
    public byte[] serialize(T obj) {
        output.clear(); 
        kryo.writeClassAndObject(output, obj);
        return output.toBytes();
    }

    @Override
    public T deserialize(byte[] b) {
        input.setBuffer(b); 
        return (T) kryo.readClassAndObject(input); 
    }
}