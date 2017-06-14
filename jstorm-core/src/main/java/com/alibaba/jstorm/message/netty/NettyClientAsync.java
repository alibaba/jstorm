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
import backtype.storm.messaging.NettyMessage;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.Flusher;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NettyClientAsync extends NettyClient {
    private static final Logger LOG = LoggerFactory.getLogger(NettyClientAsync.class);
    public static final String PREFIX = "Netty-Client-";

    protected Flusher flusher;
    protected int flushCheckInterval;

    private boolean isBackpressureEnable;
    // Map<TargetTaskId, remoteAddress>
    private volatile Map<Integer, Boolean> targetTasksUnderFlowCtrl;
    private Map<Integer, Pair<Lock, Condition>> targetTasksToLocks;
    // Map<SourceTask, Map<TargetTask, Cache>>
    private volatile Map<Integer, Map<Integer, MessageBuffer>> targetTasksCache;
    private int flowCtrlAwaitTime;
    private int cacheSize;

    private class NettyClientFlush extends Flusher {
        private AtomicBoolean isFlushing = new AtomicBoolean(false);

        public NettyClientFlush(long flushInterval) {
            flushIntervalMs = flushInterval;
        }

        public void run() {
            if (isFlushing.compareAndSet(false, true) && !isClosed()) {
                synchronized (writeLock) {
                    MessageBatch cache = getPendingCaches();
                    Channel channel = waitForChannelReady();
                    if (channel != null) {
                        MessageBatch messageBatch = messageBuffer.drain();
                        if (messageBatch != null)
                            cache.add(messageBatch);
                        flushRequest(channel, cache);
                    }
                }
                isFlushing.set(false);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    NettyClientAsync(Map conf, ChannelFactory factory, String host, int port, ReconnectRunnable reconnector, final Set<Integer> sourceTasks, final Set<Integer> targetTasks) {
        super(conf, factory, host, port, reconnector);

        clientChannelFactory = factory;
        initFlowCtrl(conf, sourceTasks, targetTasks);
        
        flushCheckInterval = Utils.getInt(conf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 5);
        flusher = new NettyClientFlush(flushCheckInterval);
        flusher.start();

        start();
    }

    private void initFlowCtrl(Map conf, Set<Integer> sourceTasks, Set<Integer> targetTasks) {
        isBackpressureEnable = ConfigExtension.isBackpressureEnable(conf);
        flowCtrlAwaitTime = ConfigExtension.getNettyFlowCtrlWaitTime(conf);
        cacheSize = ConfigExtension.getNettyFlowCtrlCacheSize(conf) != null ? ConfigExtension.getNettyFlowCtrlCacheSize(conf) : messageBatchSize;

        targetTasksUnderFlowCtrl = new HashMap<>();
        targetTasksToLocks = new HashMap<>();
        targetTasksCache = new HashMap<>();
        for (Integer task : targetTasks) {
            targetTasksUnderFlowCtrl.put(task, false);
            Lock lock = new ReentrantLock();
            targetTasksToLocks.put(task, new Pair<>(lock, lock.newCondition()));
        }

        Set<Integer> tasks = new HashSet<Integer>(sourceTasks);
        tasks.add(0); // add task-0 as default source task
        for (Integer sourceTask : tasks) {
            Map<Integer, MessageBuffer> messageBuffers = new HashMap<>();
            for (Integer targetTask : targetTasks) {
                messageBuffers.put(targetTask, new MessageBuffer(cacheSize));
            }
            targetTasksCache.put(sourceTask, messageBuffers);
        }
    }

    @Override
    public void send(List<TaskMessage> messages) {
        // throw exception if the client is being closed
        if (isClosed()) {
            LOG.warn("Client is being closed, and does not take requests any more");
            return;
        }

        long start = enableNettyMetrics && sendTimer != null ? sendTimer.getTime() : 0L;
        try {
            for (TaskMessage message : messages) {
                waitforFlowCtrlAndSend(message);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sendTimer != null && enableNettyMetrics) {
                sendTimer.updateTime(start);
            }
        }
    }

    @Override
    public void send(TaskMessage message) {
        // throw exception if the client is being closed
        if (isClosed()) {
            LOG.warn("Client is being closed, and does not take requests any more");
        } else {
            long start = enableNettyMetrics && sendTimer != null ? sendTimer.getTime() : 0L;
            try {
                waitforFlowCtrlAndSend(message);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (sendTimer != null && enableNettyMetrics) {
                    sendTimer.updateTime(start);
                }
            }
        }
    }

    @Override
    public void sendDirect(TaskMessage message) {
        synchronized (writeLock) {
            Channel channel = waitForChannelReady();
            if (channel != null)
                flushRequest(channel, message);
        }
    }

    void pushBatch(NettyMessage message) {
        if (message == null || message.isEmpty()) {
            return;
        }

        synchronized (writeLock) {
            Channel channel = channelRef.get();
            if (channel == null) {
                messageBuffer.add(message, false);
                LOG.debug("Pending requested message, the size is {}, because channel is not ready.", messageBuffer.size());
            } else {
                if (channel.isWritable()) {
                    MessageBatch messageBatch = messageBuffer.add(message);
                    if (messageBatch != null) {
                        flushRequest(channel, messageBatch);
                    }
                } else {
                    messageBuffer.add(message, false);
                }
            }

            if (messageBuffer.size() >= BATCH_THRESHOLD_WARN) {
                channel = waitForChannelReady();
                if (channel != null) {
                    MessageBatch messageBatch = messageBuffer.drain();
                    flushRequest(channel, messageBatch);
                }
            }
        }
    }

    private boolean discardCheck(long pendingTime, long timeoutMs, int messageSize) {
        if (timeoutMs != -1 && pendingTime >= timeoutMs) {
            LOG.warn("Discard message due to pending message timeout({}ms), messageSize={}", timeoutMs, messageSize);
            return true;
        } else {
            return false;
        }
    }

    public Channel waitForChannelReady() {
        Channel channel = channelRef.get();
        long pendingTime = 0;
        while ((channel == null && !isClosed()) || (channel != null && !channel.isWritable())) {
            JStormUtils.sleepMs(1);
            pendingTime++;
            if (discardCheck(pendingTime, timeoutMs, messageBuffer.size())) {
                messageBuffer.clear();
                return null;
            }
            if (pendingTime % 30000 == 0) {
                LOG.info("Pending total time={}, channel.isWritable={}, pendingNum={}, remoteAddress={}", pendingTime, channel != null ? channel.isWritable()
                        : null, pendings.get(), channel != null ? channel.getRemoteAddress() : null);
            }
            channel = channelRef.get();
        }
        return channel;
    }

    @Override
    public void handleResponse(Channel channel, Object msg) {
        if (msg == null) {
            return;
        }

        TaskMessage message = (TaskMessage) msg;
        short type = message.get_type();
        if (type == TaskMessage.BACK_PRESSURE_REQUEST) {
            byte[] messageData = message.message();
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE + 1);
            buffer.put(messageData);
            buffer.flip();
            boolean startFlowCtrl = buffer.get() == 1;
            int targetTaskId = buffer.getInt();

            //LOG.info("Received flow ctrl ({}) for target task-{}", startFlowCtrl, targetTaskId);
            if (startFlowCtrl) {
                addFlowControl(channel, targetTaskId);
            } else {
                Pair<Lock, Condition> pair = removeFlowControl(targetTaskId);
                /*if (pair != null) {
                    try {
                        pair.getFirst().lock();
                        pair.getSecond().signalAll();
                    } finally {
                        pair.getFirst().unlock();
                    }
                }*/
            }
        } else {
            LOG.warn("Unexpected message (type={}) was received from task {}", type, message.task());
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    private void addFlowControl(Channel channel, int taskId) {
        targetTasksUnderFlowCtrl.put(taskId, true);
    }

    private Pair<Lock, Condition> removeFlowControl(int taskId) {
        targetTasksUnderFlowCtrl.put(taskId, false);
        return targetTasksToLocks.get(taskId);
    }

    private boolean isUnderFlowCtrl(int taskId) {
        return targetTasksUnderFlowCtrl.get(taskId);
    }

    private MessageBuffer getCacheBuffer(int sourceTaskId, int targetTaskId) {
        return targetTasksCache.get(sourceTaskId).get(targetTaskId);
    }

    private MessageBatch flushCacheBatch(int sourceTaskId, int targetTaskId) {
        MessageBatch batch = null;
        MessageBuffer buffer = getCacheBuffer(sourceTaskId, targetTaskId);
        synchronized (buffer) {
            batch = buffer.drain();
        }
        return batch;
    }

    private MessageBatch addMessageIntoCache(int sourceTaskId, int targetTaskId, TaskMessage message) {
        MessageBatch batch = null;
        MessageBuffer buffer = getCacheBuffer(sourceTaskId, targetTaskId);
        synchronized (buffer) {
            batch = buffer.add(message);
        }
        return batch;
    }

    private MessageBatch getPendingCaches() {
        MessageBatch ret = new MessageBatch(cacheSize);
        for (Entry<Integer, Map<Integer, MessageBuffer>> entry : targetTasksCache.entrySet()) {
            int sourceTaskId = entry.getKey();
            Map<Integer, MessageBuffer> MessageBuffers = entry.getValue();
            for (Integer targetTaskId : MessageBuffers.keySet()) {
                if (!isUnderFlowCtrl(targetTaskId)) {
                    MessageBatch batch = flushCacheBatch(sourceTaskId, targetTaskId);
                    if (batch != null)
                        ret.add(batch);
                }
            }
        }
        return ret;
    }

    private void waitforFlowCtrlAndSend(TaskMessage message) {
        // If backpressure is disable, just send directly.
        if (!isBackpressureEnable) {
            pushBatch(message);
            return;
        }

        int sourceTaskId = message.sourceTask();
        int targetTaskId = message.task();
        if (isUnderFlowCtrl(targetTaskId)) {
            // If target task is under flow control
            MessageBatch flushCache = addMessageIntoCache(sourceTaskId, targetTaskId, message);
            if (flushCache != null) {
                // Cache is full. Try to flush till flow control is released.
                /*Pair<Lock, Condition> pair = targetTasksToLocks.get(targetTaskId);
                long pendingTime = 0;
                while (isUnderFlowCtrl(targetTaskId)) {
                    try {
                        pair.getFirst().lock();
                        if(pair.getSecond().await(flowCtrlAwaitTime, TimeUnit.MILLISECONDS))
                            break;
                    } catch (InterruptedException e) {
                        LOG.info("flow control was interrupted! targetTask-{}", targetTaskId);
                    } finally {
                        pair.getFirst().unlock();
                    }

                    pendingTime += flowCtrlAwaitTime;
                    if (discardCheck(pendingTime, timeoutMs, flushCache.getEncodedLength())) {
                        removeFlowControl(targetTaskId);
                        return;
                    }
                    if (pendingTime % 30000 == 0) {
                        LOG.info("Pending total time={} since target task-{} is under flow control ", pendingTime, targetTaskId);
                    }
                }*/
                long pendingTime = 0;
                while (isUnderFlowCtrl(targetTaskId)) {
                    JStormUtils.sleepMs(1);
                    pendingTime++;
                    if (pendingTime % 30000 == 0) {
                        LOG.info("Pending total time={} since target task-{} is under flow control ", pendingTime, targetTaskId);
                    }
                }
                pushBatch(flushCache);
            }
        } else {
            MessageBatch cache = flushCacheBatch(sourceTaskId, targetTaskId);
            if (cache != null) {
                cache.add(message);
                pushBatch(cache);
            } else {
                pushBatch(message);
            }
        }
    }

    private void releaseFlowCtrlsForRemoteAddr(String remoteAddr) {
        LOG.info("Release flow control for remoteAddr={}", remoteAddr);
        for (Entry<Integer, Boolean> entry : targetTasksUnderFlowCtrl.entrySet()) {
            entry.setValue(false);
        }
    }

    @Override
    public void disconnectChannel(Channel channel) {
        releaseFlowCtrlsForRemoteAddr(channel.getRemoteAddress().toString());
        if (isClosed()) {
            return;
        }

        if (channel == channelRef.get()) {
            setChannel(null);
            reconnect();
        } else {
            closeChannel(channel);
        }
    }

    @Override
    public boolean available(int taskId) {
        return super.available(taskId) && !isUnderFlowCtrl(taskId);
    }

    @Override
    public void close() {
        flusher.close();
        super.close();
    }
}
