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