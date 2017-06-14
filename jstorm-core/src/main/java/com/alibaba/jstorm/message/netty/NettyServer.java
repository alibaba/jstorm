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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

class NettyServer implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);
    @SuppressWarnings("rawtypes")
    Map stormConf;
    int port;

    // private LinkedBlockingQueue message_queue;
    volatile StormChannelGroup allChannels = new StormChannelGroup("jstorm-server");
    final ChannelFactory factory;
    final ServerBootstrap bootstrap;

    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;
    private DisruptorQueue recvControlQueue;

    private boolean isBackpressureEnable;
    private NettyServerFlowCtrlHandler flowCtrlHandler;
    private volatile boolean bstartRec;
    private final Set<Integer> workerTasks;

    @SuppressWarnings("rawtypes")
    NettyServer(Map stormConf, int port, ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues, DisruptorQueue recvControlQueue, boolean bstartRec,
            Set<Integer> workerTasks) {
        this.stormConf = stormConf;
        this.port = port;
        this.deserializeQueues = deserializeQueues;
        this.recvControlQueue = recvControlQueue;
        this.bstartRec = bstartRec;
        this.workerTasks = workerTasks;

        // Configure the server.
        int buffer_size = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_RECEIVE_BUFFER_SIZE));
        int maxWorkers = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

        // asyncBatch = ConfigExtension.isNettyTransferAsyncBatch(storm_conf);

        ThreadFactory bossFactory = new NettyRenameThreadFactory("server" + "-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("server" + "-worker");
        if (maxWorkers > 0) {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), Executors.newCachedThreadPool(workerFactory));
        }

        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.receiveBufferSize", buffer_size);
        bootstrap.setOption("child.keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormServerPipelineFactory(this, stormConf));

        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);

        LOG.info("Successfully bind {}, buffer_size:{}, maxWorkers:{}", port, buffer_size, maxWorkers);

        this.isBackpressureEnable = ConfigExtension.isBackpressureEnable(stormConf);
        if (isBackpressureEnable) {
            flowCtrlHandler = new NettyServerFlowCtrlHandler(stormConf, allChannels, workerTasks);
            flowCtrlHandler.start();
        }
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {
        deserializeQueues.put(taskId, recvQueu);
    }

    /**
     * enqueue a received message
     */
    public void enqueue(TaskMessage message, Channel channel) {
        // lots of messages may be lost when deserialize queue hasn't finished init operation
        while (!bstartRec) {
            LOG.info("check whether deserialize queues have already been created");
            boolean isFinishInit = true;
            for (Integer task : workerTasks) {
                if (deserializeQueues.get(task) == null) {
                    isFinishInit = false;
                    JStormUtils.sleepMs(10);
                    break;
                }
            }
            if (isFinishInit) {
                bstartRec = isFinishInit;
            }
        }
        short type = message.get_type();
        if (type == TaskMessage.NORMAL_MESSAGE) {
            // enqueue a received message
            int task = message.task();
            DisruptorQueue queue = deserializeQueues.get(task);
            if (queue == null) {
                LOG.warn("Received invalid message directed at task {}. Dropping...", task);
                LOG.debug("Message data: {}", JStormUtils.toPrintableString(message.message()));
                return;
            }
            if (!isBackpressureEnable) {
                queue.publish(message.message());
            } else {
                flowCtrlHandler.flowCtrl(channel, queue, task, message.message());
            }
        } else if (type == TaskMessage.CONTROL_MESSAGE) {
            // enqueue a control message
            if (recvControlQueue == null) {
                LOG.info("Can not find the recvControlQueue. Dropping this control message");
                return;
            }
            recvControlQueue.publish(message);
        } else {
            LOG.warn("Unexpected message (type={}) was received from task {}", type, message.task());
        }
    }

    /**
     * fetch a message from message queue synchronously (flags != 1) or asynchronously (flags==1)
     */
    public Object recv(Integer taskId, int flags) {
        try {
            DisruptorQueue recvQueue = deserializeQueues.get(taskId);
            if ((flags & 0x01) == 0x01) {
                return recvQueue.poll();
                // non-blocking
            } else {
                return recvQueue.take();
            }
        } catch (Exception e) {
            LOG.warn("Unknown exception ", e);
            return null;
        }
    }

    /**
     * register a newly created channel
     */
    protected void addChannel(Channel channel) {
        allChannels.add(channel);
    }

    /**
     * close a channel
     */
    protected void closeChannel(Channel channel) {
        MessageDecoder.removeTransmitHistogram(channel);
        channel.close().awaitUninterruptibly();
        allChannels.remove(channel);
    }

    /**
     * close all channels, and release resources
     */
    public void close() {
        LOG.info("Begin to shutdown NettyServer");
        if (allChannels != null) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        // await(5, TimeUnit.SECONDS)
                        // sometimes allChannels.close() will block the exit thread
                        allChannels.close().await(1, TimeUnit.SECONDS);
                        LOG.info("Successfully close all channel");
                        factory.releaseExternalResources();
                    } catch (Exception ignored) {
                    }
                    allChannels = null;
                }
            }).start();

            JStormUtils.sleepMs(1000);
        }
        LOG.info("Successfully shutdown NettyServer");
    }

    @Override
    public void send(List<TaskMessage> messages) {
        throw new UnsupportedOperationException("Server connection should not send any message");
    }

    @Override
    public void send(TaskMessage message) {
        throw new UnsupportedOperationException("Server connection should not send any message");
    }

    @Override
    public void sendDirect(TaskMessage message) {
        throw new UnsupportedOperationException("Server connection should not send any message");
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean available(int taskId) {
        return true;
    }

    public StormChannelGroup getChannelGroup() {
        return allChannels;
    }
}
