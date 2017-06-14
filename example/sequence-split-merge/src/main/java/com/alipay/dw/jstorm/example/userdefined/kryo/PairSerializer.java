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
