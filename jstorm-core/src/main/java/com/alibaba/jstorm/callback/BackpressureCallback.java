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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.message.netty.StormChannelGroup;

public class BackpressureCallback implements Callback {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureCallback.class);

    private StormChannelGroup stormChannelGroup;
    private DisruptorQueue monitorQueue;
    private float lowMark;
    private int taskId;
    private Map<Integer, HashSet<String>> remoteClientsUnderFlowCtrl;

    public BackpressureCallback(StormChannelGroup channelGroup, DisruptorQueue queue, float lowMark, int taskId,
    		Map<Integer, HashSet<String>> remoteClientsUnderFlowCtrl) {
        this.stormChannelGroup = channelGroup;
        this.monitorQueue = queue;
        this.lowMark = lowMark;
        this.taskId = taskId;
        this.remoteClientsUnderFlowCtrl = remoteClientsUnderFlowCtrl;
    }

    public <T> Object execute(T... args) {
        if (monitorQueue.pctFull() < lowMark && monitorQueue.cacheSize() == 0) {
        	HashSet<String> remoteAddrs = remoteClientsUnderFlowCtrl.get(taskId);
            //LOG.debug("Release flow ctrl for task-{} for {}", taskId, remoteAddrs);

            synchronized (remoteAddrs) {
           	    for (String remoteAddr : remoteAddrs) {
           	        Channel channel = stormChannelGroup.getChannel(remoteAddr);
           	        if (channel == null) {
           	        	continue;
           	        }
                    // send back backpressure flow control request to source client
                    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE + 1);
                    buffer.put((byte) 0); // 1-> start flow control; 0-> stop flow control
                    buffer.putInt(taskId);
                    TaskMessage flowCtrlMsg = new TaskMessage(TaskMessage.BACK_PRESSURE_REQUEST, 0, buffer.array());
                    channel.write(flowCtrlMsg);
           	    }
           	    remoteAddrs.clear();
            }
            return true;
        } else {
            return false;
        }
    }

}
