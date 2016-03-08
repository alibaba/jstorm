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

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class NettyClientAsync extends NettyClient {
    private static final Logger LOG = LoggerFactory.getLogger(NettyClientAsync.class);
    public static final String PREFIX = "Netty-Client-";

    // when batch buffer size is more than BATCH_THREASHOLD_WARN
    // it will block Drainer thread
    protected long BATCH_THREASHOLD_WARN;
    protected final boolean directlySend;

    protected AtomicBoolean flush_later;
    protected int flushCheckInterval;
    protected final boolean blockSend;

    boolean isDirectSend(Map conf) {
        if (JStormServerUtils.isOnePending(conf)) {
            return true;
        }

        return !ConfigExtension.isNettyTransferAsyncBatch(conf);
    }

    boolean isBlockSend(Map storm_conf) {
        if (!ConfigExtension.isTopologyContainAcker(storm_conf)) {
            return false;
        }

        return ConfigExtension.isNettyASyncBlock(storm_conf);
    }

    @SuppressWarnings("rawtypes")
    NettyClientAsync(Map storm_conf, ChannelFactory factory, ScheduledExecutorService scheduler, String host, int port, ReconnectRunnable reconnector) {
        super(storm_conf, factory, scheduler, host, port, reconnector);

        BATCH_THREASHOLD_WARN = ConfigExtension.getNettyBufferThresholdSize(storm_conf);
        blockSend = isBlockSend(storm_conf);
        directlySend = isDirectSend(storm_conf);

        flush_later = new AtomicBoolean(false);
        flushCheckInterval = Utils.getInt(storm_conf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 10);

        Runnable flusher = new Runnable() {
            @Override
            public void run() {
                flush();
            }
        };
        long initialDelay = Math.min(1000, max_sleep_ms * max_retries);
        scheduler.scheduleAtFixedRate(flusher, initialDelay, flushCheckInterval, TimeUnit.MILLISECONDS);

        clientChannelFactory = factory;

        start();
        LOG.info(this.toString());
    }

    /**
     * Enqueue a task message to be sent to server
     */
    @Override
    public synchronized void send(List<TaskMessage> messages) {
        // throw exception if the client is being closed
        if (isClosed()) {
            LOG.warn("Client is being closed, and does not take requests any more");
            return;
        }

        long start = enableNettyMetrics ? sendTimer.getTime() : 0L;
        try {
            pushBatch(messages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sendTimer != null && enableNettyMetrics) {
                sendTimer.updateTime(start);
            }
        }
    }

    @Override
    public synchronized void send(TaskMessage message) {
        // throw exception if the client is being closed
        if (isClosed()) {
            LOG.warn("Client is being closed, and does not take requests any more");
            return;
        }

        long start = enableNettyMetrics ? sendTimer.getTime() : 0L;
        try {
            pushBatch(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sendTimer != null && enableNettyMetrics) {
                sendTimer.updateTime(start);
            }
        }
    }

    void waitChannelReady(long cachedSize, long sleepMs) {
        long begin = System.currentTimeMillis();
        boolean changeThreadhold = false;
        IntervalCheck oneSecond = new IntervalCheck();
        IntervalCheck timeoutIntervalCheck = new IntervalCheck();
        timeoutIntervalCheck.setIntervalMs(timeoutMs);
        sleepMs = (sleepMs < timeoutMs) ? sleepMs : timeoutMs;
        while (isChannelReady() == null) {

            long now = System.currentTimeMillis();
            long delt = now - begin;
            if (oneSecond.check()) {
                LOG.warn("Target server  {} is unavailable, pending {}, bufferSize {}, block sending {}ms", name, pendings.get(), cachedSize, delt);
            }

            if (timeoutIntervalCheck.check()) {
                if (messageBatchRef.get() != null) {
                    LOG.warn("Target server  {} is unavailable, wait too much time, throw timeout message", name);
                    messageBatchRef.set(null);
                }
                setChannel(null);
                LOG.warn("Reset channel as null");

                if (!blockSend) {
                    reconnect();
                    break;
                }
            }

            reconnect();
            JStormUtils.sleepMs(sleepMs);

            if (delt > 2 * timeoutMs * 1000L && !changeThreadhold) {
                if (channelRef.get() != null && BATCH_THREASHOLD_WARN >= 2 * messageBatchSize) {
                    // it is just channel isn't writable;
                    BATCH_THREASHOLD_WARN = BATCH_THREASHOLD_WARN / 2;
                    LOG.info("Reduce BATCH_THREASHOLD_WARN to {}", BATCH_THREASHOLD_WARN);

                    changeThreadhold = true;
                }
            }

            if (isClosed()) {
                LOG.info("Channel has been closed " + name());
                break;
            }
        }
    }

    long getDelaySec(long cachedSize) {
        long count = cachedSize / BATCH_THREASHOLD_WARN;
        long sleepMs = (long) (Math.pow(2, count) * 10);

        if (sleepMs > 1000) {
            sleepMs = 1000;
        }

        return sleepMs;
    }

    void handleFailedChannel(MessageBatch messageBatch) {

        messageBatchRef.set(messageBatch);
        flush_later.set(true);

        long cachedSize = messageBatch.getEncoded_length();
        if (cachedSize > BATCH_THREASHOLD_WARN) {
            long sleepMs = getDelaySec(cachedSize);
            waitChannelReady(cachedSize, sleepMs);
        }
    }

    void pushBatch(List<TaskMessage> messages) {

        if (messages.isEmpty()) {
            return;
        }

        MessageBatch messageBatch = messageBatchRef.getAndSet(null);
        if (null == messageBatch) {
            messageBatch = new MessageBatch(messageBatchSize);
        }

        for (TaskMessage message : messages) {
            if (TaskMessage.isEmpty(message)) {
                continue;
            }

            messageBatch.add(message);

            if (messageBatch.isFull()) {
                Channel channel = isChannelReady();
                if (channel != null) {
                    flushRequest(channel, messageBatch);

                    messageBatch = new MessageBatch(messageBatchSize);
                }

            }
        }

        Channel channel = isChannelReady();
        if (channel == null) {
            handleFailedChannel(messageBatch);
        } else if (!messageBatch.isEmpty()) {
            flushRequest(channel, messageBatch);
        }
    }

    void pushBatch(TaskMessage message) {

        if (TaskMessage.isEmpty(message)) {
            return;
        }

        MessageBatch messageBatch = messageBatchRef.getAndSet(null);
        if (null == messageBatch) {
            messageBatch = new MessageBatch(messageBatchSize);
        }

        messageBatch.add(message);

        Channel channel = isChannelReady();
        if (channel == null) {
            handleFailedChannel(messageBatch);
            return;
        }

        if (messageBatch.isFull()) {
            flushRequest(channel, messageBatch);

            return;
        }

        if (directlySend) {
            flushRequest(channel, messageBatch);
        } else {
            if (messageBatchRef.compareAndSet(null, messageBatch)) {
                flush_later.set(true);
            } else
                LOG.error("MessageBatch will be lost. This should not happen.");
        }
    }

    void flush() {
        if (isClosed()) {
            return;
        }

        if (!flush_later.get()) {
            return;
        }

        Channel channel = isChannelReady();
        if (channel == null) {
            return;
        }

        flush_later.set(false);
        MessageBatch toBeFlushed = messageBatchRef.getAndSet(null);
        flushRequest(channel, toBeFlushed);
    }

    @Override
    Channel isChannelReady() {
        Channel channel = super.isChannelReady();
        if (channel == null) {
            return null;
        }

        if (blockSend && pendings.get() >= MAX_SEND_PENDING) {
            return null;
        }
        return channel;
    }

    @Override
    public void handleResponse() {
        // do nothing
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
