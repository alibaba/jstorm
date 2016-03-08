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
import backtype.storm.messaging.ControlMessage;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Gauge;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

class NettyClientSync extends NettyClient implements EventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NettyClientSync.class);

    private ConcurrentLinkedQueue<MessageBatch> batchQueue;
    private DisruptorQueue disruptorQueue;
    private ExecutorService bossExecutor;
    private ExecutorService workerExecutor;

    private AtomicLong emitTs = new AtomicLong(0);

    @SuppressWarnings("rawtypes")
    NettyClientSync(Map storm_conf, ChannelFactory factory, ScheduledExecutorService scheduler, String host, int port, ReconnectRunnable reconnector) {
        super(storm_conf, factory, scheduler, host, port, reconnector);

        batchQueue = new ConcurrentLinkedQueue<MessageBatch>();

        WaitStrategy waitStrategy = (WaitStrategy) Utils.newInstance((String) storm_conf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));

        disruptorQueue = DisruptorQueue.mkInstance(name, ProducerType.MULTI, MAX_SEND_PENDING * 8, waitStrategy);
        //disruptorQueue.consumerStarted();

        if (!connectMyself) {
            registerSyncMetrics();
        }

        Runnable trigger = new Runnable() {
            @Override
            public void run() {
                trigger();
            }
        };

        scheduler.scheduleAtFixedRate(trigger, 10, 1, TimeUnit.SECONDS);

        /**
         * In sync mode, it can't directly use common factory, it will occur problem when client close and restart
         */
        ThreadFactory bossFactory = new NettyRenameThreadFactory(MetricDef.NETTY_CLI + JStormServerUtils.getName(host, port) + "-boss");
        bossExecutor = Executors.newCachedThreadPool(bossFactory);
        ThreadFactory workerFactory = new NettyRenameThreadFactory(MetricDef.NETTY_CLI + JStormServerUtils.getName(host, port) + "-worker");
        workerExecutor = Executors.newCachedThreadPool(workerFactory);

        clientChannelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor, 1);

        start();

        LOG.info(this.toString());
    }

    public void registerSyncMetrics() {
        if (enableNettyMetrics) {
            JStormMetrics.registerNettyMetric(MetricUtils
                            .nettyMetricName(MetricDef.NETTY_CLI_SYNC_BATCH_QUEUE + nettyConnection.toString(), MetricType.GAUGE),
                    new AsmGauge(new Gauge<Double>() {
                        @Override
                        public Double getValue() {
                            return (double) batchQueue.size();
                        }
                    }));

            QueueGauge cacheQueueGauge = new QueueGauge(disruptorQueue, MetricDef.NETTY_CLI_SYNC_DISR_QUEUE, nettyConnection.toString());

            JStormMetrics.registerNettyMetric(MetricUtils
                            .nettyMetricName(MetricDef.NETTY_CLI_SYNC_DISR_QUEUE + nettyConnection.toString(), MetricType.GAUGE),
                    new AsmGauge(cacheQueueGauge));
            JStormHealthCheck.registerWorkerHealthCheck(
                    MetricDef.NETTY_CLI_SYNC_DISR_QUEUE + ":" + nettyConnection.toString(), cacheQueueGauge);
        }
    }

    /**
     * Enqueue a task message to be sent to server
     */
    @Override
    public void send(List<TaskMessage> messages) {
        for (TaskMessage msg : messages) {
            disruptorQueue.publish(msg);
        }
    }

    @Override
    public void send(TaskMessage message) {
        disruptorQueue.publish(message);
    }

    public void flushBatch(MessageBatch batch, Channel channel) {
        emitTs.set(System.currentTimeMillis());
        if (batch == null) {
            LOG.warn("Handle no data to {}, this shouldn't occur", name);

        } else if (channel == null || !channel.isWritable()) {
            LOG.warn("Channel occur exception, during batch messages {}", name);
            batchQueue.offer(batch);
        } else {

            flushRequest(channel, batch);
        }
    }

    /**
     * Don't take care of competition
     */
    public void sendData() {
        long start = enableNettyMetrics ? sendTimer.getTime() : 0;
        try {
            MessageBatch batch = batchQueue.poll();
            if (batch == null) {

                disruptorQueue.consumeBatchWhenAvailable(this);

                batch = batchQueue.poll();
            }

            Channel channel = channelRef.get();
            flushBatch(batch, channel);
        } catch (Throwable e) {
            LOG.error("Occur e", e);
            String err = name + " nettyclient occur unknow exception";
            JStormUtils.halt_process(-1, err);
        } finally {
            if (sendTimer != null && enableNettyMetrics) {
                sendTimer.updateTime(start);
            }
        }
    }

    public void sendAllData() {
        long start = enableNettyMetrics ? sendTimer.getTime() : 0L;
        try {
            disruptorQueue.consumeBatch(this);
            MessageBatch batch = batchQueue.poll();
            while (batch != null) {
                Channel channel = channelRef.get();
                if (channel == null) {
                    LOG.info("No channel {} to flush all data", name);
                    return;
                } else if (!channel.isWritable()) {
                    LOG.info("Channel {} is no writable", name);
                    return;
                }
                flushBatch(batch, channel);
                batch = batchQueue.poll();
            }
        } catch (Throwable e) {
            LOG.error("Occur e", e);
            String err = name + " nettyclient occur unknow exception";
            JStormUtils.halt_process(-1, err);
        } finally {
            if (sendTimer != null && enableNettyMetrics) {
                sendTimer.updateTime(start);
            }
        }
    }

    @Override
    public void handleResponse() {
        emitTs.set(0);
        sendData();
    }

    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        if (event == null) {
            return;
        }

        MessageBatch messageBatch = messageBatchRef.getAndSet(null);
        if (null == messageBatch) {
            messageBatch = new MessageBatch(messageBatchSize);
        }

        messageBatch.add(event);

        if (messageBatch.isFull()) {
            batchQueue.offer(messageBatch);
        } else if (endOfBatch) {
            batchQueue.offer(messageBatch);
        } else {
            messageBatchRef.set(messageBatch);
        }
    }

    /**
     * Handle lost message case
     */
    void trigger() {
        if (isClosed()) {
            return;
        }

        // if long time no receive NettyServer response
        // it is likely lost message
        long emitTime = emitTs.get();
        if (emitTime == 0) {
            return;
        }

        long now = System.currentTimeMillis();

        long delt = now - emitTime;
        if (delt < timeoutMs) {
            return;
        }

        Channel channel = channelRef.get();
        if (channel != null) {
            LOG.info("Long time no response of {}, {}s", name, delt / 1000);
            channel.write(ControlMessage.EOB_MESSAGE);
        }

    }

    protected void shutdownPool() {
        bossExecutor.shutdownNow();
        workerExecutor.shutdownNow();

        try {
            bossExecutor.awaitTermination(1, TimeUnit.SECONDS);
            workerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error when shutting down client scheduler", e);
        }

        clientChannelFactory.releaseExternalResources();
    }

    public void unregisterSyncMetrics() {
        if (enableNettyMetrics) {
            JStormMetrics.unregisterNettyMetric(MetricUtils
                    .nettyMetricName(MetricDef.NETTY_CLI_SYNC_BATCH_QUEUE + nettyConnection.toString(), MetricType.GAUGE));
            JStormMetrics.unregisterNettyMetric(MetricUtils
                    .nettyMetricName(MetricDef.NETTY_CLI_SYNC_DISR_QUEUE + nettyConnection.toString(), MetricType.GAUGE));
            JStormHealthCheck
                    .unregisterWorkerHealthCheck(MetricDef.NETTY_CLI_SYNC_DISR_QUEUE + ":" + nettyConnection.toString());
        }
    }

    @Override
    public void close() {
        LOG.info("Begin to close connection to {} and flush all data, batchQueue {}, disruptor {}", name, batchQueue.size(), disruptorQueue.population());
        sendAllData();
        disruptorQueue.haltWithInterrupt();
        if (!connectMyself) {
            unregisterSyncMetrics();
        }

        super.close();

        shutdownPool();

    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
