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
package com.alipay.dw.jstorm.example.sequence.kryo;

import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class TradeCustomerSerializer extends Serializer<TradeCustomer> {

    PairSerializer pairSerializer = new PairSerializer();

    @Override
    public TradeCustomer read(Kryo kryo, Input input, Class<TradeCustomer> arg2) {

        Pair custormer = kryo.readObject(input, Pair.class);
        Pair trade = kryo.readObject(input, Pair.class);

        long timeStamp = input.readLong();
        String buffer = input.readString();

        TradeCustomer inner = new TradeCustomer(timeStamp, trade, custormer, buffer);
        return inner;
    }

    @Override
    public void write(Kryo kryo, Output output, TradeCustomer inner) {

        kryo.writeObject(output, inner.getCustomer());
        kryo.writeObject(output, inner.getTrade());
        output.writeLong(inner.getTimestamp());
        output.writeString(inner.getBuffer());
    }

}