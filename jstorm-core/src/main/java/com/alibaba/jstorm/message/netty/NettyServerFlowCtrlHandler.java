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
package com.alibaba.jstorm.message.netty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.BackpressureCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

public class NettyServerFlowCtrlHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerFlowCtrlHandler.class);

    private StormChannelGroup allChannels;
    private Map<Integer, Set<String>> flowCtrlClients;
    private float lowMark;
    private float highMark;
    private BlockingQueue<Integer> eventQueue;

    private class FlowCtrlChecker implements Runnable {
        public void run() {
            while(true) {
                int taskId;
                try {
                    taskId = eventQueue.take();
                    Set<String> remoteAddrs = flowCtrlClients.get(taskId);
                    if (remoteAddrs == null)
                        continue;

                    synchronized (remoteAddrs) {
                        if (!remoteAddrs.isEmpty()) {
                            for (String remoteAddr : remoteAddrs) {
                                Channel channel = allChannels.getChannel(remoteAddr);
                                if (channel == null) {
                                    continue;
                                }
                                // send back backpressure flow control request to source client
                                JStormUtils.sendFlowControlRequest(channel, taskId, 0);
                            }
                            remoteAddrs.clear();
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Failed to take flow control event", e);
                }
            }
        }
    }

    public NettyServerFlowCtrlHandler (Map stormConf, StormChannelGroup allChannels, Set<Integer> localWorkerTasks) {
        this.allChannels = allChannels;
        this.flowCtrlClients = new HashMap<>();
        for (Integer taskId : localWorkerTasks) {
            flowCtrlClients.put(taskId, new HashSet<String>());
        }
        this.highMark = (float) ConfigExtension.getBackpressureWaterMarkHigh(stormConf);
        this.lowMark = (float) ConfigExtension.getBackpressureWaterMarkLow(stormConf);
        LOG.info("backpressureEnable: highMark: {}, lowMark: {}", highMark, lowMark);
        eventQueue = new LinkedBlockingDeque<>();
    }

    public void start() {
        Thread thread = new Thread(new FlowCtrlChecker());
        thread.start();
    }

    public void flowCtrl(Channel channel, DisruptorQueue queue, int taskId, byte[] message) {
        boolean initFlowCtrl = false;
        final Set<String> remoteAddrs = flowCtrlClients.get(taskId);
        synchronized (remoteAddrs) {
            if (remoteAddrs.size() > 0) {
                queue.publishCache(message);
                if(remoteAddrs.add(channel.getRemoteAddress().toString()))
                    JStormUtils.sendFlowControlRequest(channel, taskId, 1);
            } else {
                queue.publish(message);
                if (queue.pctFull() > highMark) {
                    remoteAddrs.add(channel.getRemoteAddress().toString());
                    JStormUtils.sendFlowControlRequest(channel, taskId, 1);
                    initFlowCtrl = true;
                }
            }
        }

        if (initFlowCtrl)
            queue.publishCallback(new BackpressureCallback(this, taskId));
    }

    public boolean checkIfUnderFlowCtrl(DisruptorQueue queue) {
        return queue.pctFull() < lowMark && queue.cacheSize() == 0; 
    }

    public void releaseFlowCtrl(int taskId) throws InterruptedException {
        eventQueue.put(taskId);
    }
}