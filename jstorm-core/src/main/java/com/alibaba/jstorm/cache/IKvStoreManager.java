/*
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

import backtype.storm.task.TopologyContext;

import java.io.IOError;
import java.io.IOException;

/**
 *
 * @param <T> Checkpoint type
 */
public interface IKvStoreManager<T> {
    /**
     *
     * @param kvStoreId
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> IKvStore<K, V> getOrCreate(String kvStoreId) throws IOException;

    /**
     *
     */
    public void close();

    /**
     * restore state by the specified checkpoint
     * @param checkpoint
     */
    public void restore(T checkpoint);

    /**
     * backup state for a checkpoint id
     * @param checkpointId
     * @return
     */
    public T backup(long checkpointId);

    /**
     * checkpoint for a checkpoint id
     * @param checkpointId
     */
    public void checkpoint(long checkpointId);

    /**
     * remove the backup file both in local or remote storage for a successful checkpoint id
     * @param checkpointId successful checkpoint id
     */
    public void remove(long checkpointId);

    /**
     * init the metrics of monitor in current store manager
     * @param context
     */
    public void initMonitor(TopologyContext context);
}