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
package com.alibaba.jstorm.callback;

import java.util.Map;
import java.util.Set;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.message.netty.NettyServerFlowCtrlHandler;

public class BackpressureCallback implements Callback {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureCallback.class);

    private NettyServerFlowCtrlHandler handler;
    private int taskId;

    public BackpressureCallback(NettyServerFlowCtrlHandler handler, int taskId) {
        this.taskId = taskId;
        this.handler = handler;
    }

    public <T> Object execute(T... args) {
        DisruptorQueue queue = (DisruptorQueue) args[0];
        if (handler.checkIfUnderFlowCtrl(queue)) {
            try {
                handler.releaseFlowCtrl(taskId);
            } catch (InterruptedException e) {
                LOG.warn("Failed to release flow control for task-{}", taskId);
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

}
