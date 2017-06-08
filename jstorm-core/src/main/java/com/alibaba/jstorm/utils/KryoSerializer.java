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