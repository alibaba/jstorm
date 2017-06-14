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
package com.alibaba.jstorm.transactional.spout;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

public class ScheduleTransactionSpout extends TransactionSpout {
    private ScheduledExecutorService scheduledService = null;

    public ScheduleTransactionSpout(ITransactionSpoutExecutor spoutExecutor) {
        super(spoutExecutor);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        spoutExecutor.open(conf, context, new SpoutOutputCollector(outputCollector));

        int threadPoolNum = JStormUtils.parseInt(conf.get("transaction.schedule.thread.pool"), 1);
        int delay = JStormUtils.parseInt(conf.get("transaction.schedule.batch.delay.ms"), 1000);
        int initDelay = delay >= 30000 ? 30000 : delay;
        if (scheduledService == null) {
            scheduledService = Executors.newScheduledThreadPool(threadPoolNum);
        }
        scheduledService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                process(Operation.commit, null);
            }
        }, initDelay, delay, TimeUnit.MILLISECONDS);
    }
}