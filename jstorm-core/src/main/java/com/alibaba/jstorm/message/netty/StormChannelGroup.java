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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormChannelGroup extends DefaultChannelGroup {
    private static final Logger LOG = LoggerFactory.getLogger(StormChannelGroup.class);

    private Map<String, Channel> channelMap = new ConcurrentHashMap<>();

    public StormChannelGroup() {
        super();
    }

    public StormChannelGroup(String name) {
        super(name);
    }

    public synchronized boolean add(Channel channel) {
        if (channel != null) {
            if (channel.getRemoteAddress() != null) {
                channelMap.put(channel.getRemoteAddress().toString(), channel);
            }
            return super.add(channel);
        } else {
            return false;
        }
    }

    public synchronized boolean remove(Channel channel) {
        channelMap.remove(channel.getRemoteAddress().toString());
        return super.remove(channel);
    }

    public Set<String> getAllRemoteAddress() {
        return channelMap.keySet();
    }

    public synchronized boolean isChannelReadable(String remoteAddress) {
        Channel ch = channelMap.get(remoteAddress);
        if (ch != null) {
            return ch.isReadable();
        } else {
            return false;
        }
    }

    public synchronized boolean suspendChannel(String remoteAddress) {
        Channel ch = channelMap.get(remoteAddress);
        if (ch != null) {
            LOG.debug("Suspend channel={}", remoteAddress);
            ch.setReadable(false);
            return true;
        } else {
            LOG.debug("Channel to be suspended is null!");
            return false;
        }
    }

    public synchronized boolean resumeChannel(String remoteAddress) {
        Channel ch = channelMap.get(remoteAddress);
        if (ch != null) {
            LOG.debug("Resume channel={}", remoteAddress);
            ch.setReadable(true);
            return true;
        } else {
            LOG.debug("Channel to be resumed is null");
            return false;
        }
    }

    public Channel getChannel(String remoteAddress) {
        return channelMap.get(remoteAddress);
    }
}