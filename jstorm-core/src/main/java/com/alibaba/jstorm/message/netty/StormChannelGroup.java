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

    private Map<String, Channel> channelMap = new ConcurrentHashMap<String, Channel>();

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