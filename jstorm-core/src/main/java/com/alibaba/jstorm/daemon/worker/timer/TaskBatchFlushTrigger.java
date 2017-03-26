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
package com.alibaba.jstorm.daemon.worker.timer;

import com.alibaba.jstorm.daemon.worker.Flusher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.jstorm.task.execute.BatchCollector;

import java.util.concurrent.atomic.AtomicBoolean;

public class TaskBatchFlushTrigger extends Flusher {
    private static final Logger LOG = LoggerFactory.getLogger(TickTupleTrigger.class);

    private BatchCollector batchCollector;
    private String name;
    private AtomicBoolean _isFlushing = new AtomicBoolean(false);

    public TaskBatchFlushTrigger(int frequence, String name, BatchCollector batchCollector) {
        if (frequence <= 0) {
            LOG.warn(" The frequence of " + name + " is invalid");
            frequence = 1;
        }
        this.name = name;
        this._flushIntervalMs = frequence;
        this.batchCollector = batchCollector;
    }

    @Override
    public void run() {
        try {
            if (_isFlushing.compareAndSet(false, true)) {
                batchCollector.flush();
                _isFlushing.set(false);
            }
        } catch (Exception e) {
            LOG.warn("Failed to public timer event to " + name, e);
            return;
        }
    }

}
