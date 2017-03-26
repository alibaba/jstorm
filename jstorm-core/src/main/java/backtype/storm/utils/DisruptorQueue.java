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
package backtype.storm.utils;

import backtype.storm.metric.api.IStatefulObject;

import com.alibaba.jstorm.callback.Callback;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;

/**
 * 
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is the ability to catch up to the producer by processing tuples in batches.
 */
public abstract class DisruptorQueue implements IStatefulObject {
    public static void setUseSleep(boolean useSleep) {
        DisruptorQueueImpl.setUseSleep(useSleep);
    }

    public static DisruptorQueue mkInstance(String queueName, ProducerType producerType, int bufferSize, WaitStrategy wait, boolean isBatch,
                                            int batchSize, long flushMs) {
        return new DisruptorQueueImpl(queueName, producerType, bufferSize, wait, isBatch, batchSize, flushMs);
    }

    public abstract String getName();

    public abstract void haltWithInterrupt();

    public abstract Object poll();

    public abstract Object take();

    public abstract void consumeBatch(EventHandler<Object> handler);

    public abstract void consumeBatchWhenAvailable(EventHandler<Object> handler);

    public abstract void consumeBatchWhenAvailableWithCallback(EventHandler<Object> handler);
    
    public abstract void multiConsumeBatchWhenAvailable(EventHandler<Object> handler);

    public abstract void multiConsumeBatchWhenAvailableWithCallback(EventHandler<Object> handler);
    
    public abstract void consumeBatchWhenAvailable(EventHandler<Object> handler, boolean isSync);

    public abstract void publish(Object obj);

    public abstract void publishBatch(Object obj);

    public abstract void publish(Object obj, boolean block) throws InsufficientCapacityException;

    public abstract void clear();

    public abstract long population();

    public abstract long capacity();

    public abstract long writePos();

    public abstract long readPos();

    public abstract float pctFull();
    
    public abstract int cacheSize();

    public abstract List<Object> retreiveAvailableBatch() throws AlertException, InterruptedException, TimeoutException;

    public abstract void publishCallback(Callback cb);
    
    public abstract void publishCache(Object obj);
}
