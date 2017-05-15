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
package com.alibaba.jstorm.window;

import java.util.Collection;
import java.util.Map;

import com.alibaba.jstorm.window.TimeWindow;

public interface IKvWindowedState<K, V> {
    public V get(TimeWindow window, K key);

    public void put(TimeWindow window, K key, V value);

    public void putBatch(TimeWindow window, Map<K, V> batch);

    public Map<K, V> getBatch(TimeWindow window, Collection<K> keys);

    public Collection<K> getAllKeys(TimeWindow window);

    public Map<K, V> getBatch(TimeWindow window);

    public void remove(TimeWindow window, K key);

    public Collection<TimeWindow> listWindows();

    public void removeWindow(TimeWindow window);

    public void clear();
}