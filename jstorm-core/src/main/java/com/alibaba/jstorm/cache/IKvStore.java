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
package com.alibaba.jstorm.cache;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface IKvStore<K, V> {

    V get(K key) throws IOException;

    Map<K, V> getBatch(Collection<K> keys) throws IOException;

    void put(K key, V value) throws IOException;

    void putBatch(Map<K, V> batch) throws IOException;
    
    void remove(K key) throws IOException;

    void removeBatch(Collection<K> keys) throws IOException;

    Collection<K> listKeys() throws IOException;

    void close() throws IOException;
}
