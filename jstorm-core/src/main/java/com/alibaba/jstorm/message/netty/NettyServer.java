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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.BackpressureCallback;
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

    private Lock lock;
    private Map<Integer, HashSet<String>> remoteClientsUnderFlowCtrl;
    private boolean isBackpressureEnable;
    private float lowMark;
    private float highMark;
    private volatile boolean bstartRec;

    private final Set<Integer> workerTasks;
    

    @SuppressWarnings("rawtypes")
    NettyServer( Map stormConf, int port, ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues,
                DisruptorQueue recvControlQueue, boolean bstartRec, Set<Integer> workerTasks) {
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
        bootstrap.setOption("reuserAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.receiveBufferSize", buffer_size);
        bootstrap.setOption("child.keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormServerPipelineFactory(this, stormConf));

        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);

        LOG.info("Successfull bind {}, buffer_size:{}, maxWorkers:{}", port, buffer_size, maxWorkers);

        this.lock = new ReentrantLock();
        this.remoteClientsUnderFlowCtrl = new HashMap<Integer, HashSet<String>>();
        for (Integer taskId : workerTasks) {
        	remoteClientsUnderFlowCtrl.put(taskId, new HashSet<String>());
        }
        this.isBackpressureEnable = ConfigExtension.isBackpressureEnable(stormConf);
        this.highMark = (float) ConfigExtension.getBackpressureWaterMarkHigh(stormConf);
        this.lowMark = (float) ConfigExtension.getBackpressureWaterMarkLow(stormConf);
        LOG.info("isBackpressureEnable: {}, highMark: {}, lowMark: {}", isBackpressureEnable, highMark, lowMark);
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {
        deserializeQueues.put(taskId, recvQueu);
    }

    private void sendFlowCtrlResp(Channel channel, int taskId) {
    	//channel.setReadable(false);

    	// send back backpressure flow control request to source client
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE + 1);
        buffer.put((byte) 1); // 1-> start flow control; 0-> stop flow control
        buffer.putInt(taskId);
        TaskMessage flowCtrlMsg = new TaskMessage(TaskMessage.BACK_PRESSURE_REQUEST, 1, buffer.array());
        channel.write(flowCtrlMsg);
        //LOG.debug("Send flow ctrl resp to address({}) for task-{}", channel.getRemoteAddress().toString(), taskId);

        //channel.setReadable(true);
    }

    private void flowCtrl(Channel channel, String addr, DisruptorQueue queue, int taskId, byte[] message) {
    	boolean isFlowCtrl = false;
    	boolean isInitFlowCtrl = false;
    	if (queue.pctFull() > lowMark || queue.cacheSize() > 0) {
    	    HashSet<String> remoteAddrs = remoteClientsUnderFlowCtrl.get(taskId);
    	    synchronized (remoteAddrs) {
    	    	if (remoteAddrs.isEmpty()) {
    	    		if (queue.pctFull() >= highMark) {
                        remoteAddrs.add(addr);
                        isInitFlowCtrl = true;
                        sendFlowCtrlResp(channel, taskId);
                        isFlowCtrl = true;
    	    		}
    	        } else if (!remoteAddrs.contains(addr)) {
    	        	remoteAddrs.add(addr);
    	        	sendFlowCtrlResp(channel, taskId);
    	        	isFlowCtrl = true;
    	    	} else {
    	    		isFlowCtrl = true;
    	    	}
    	    }
        }

    	if (isFlowCtrl) {
	    	queue.publishCache(message);
	        if (isInitFlowCtrl) {
	    	    queue.publishCallback(new BackpressureCallback(allChannels, queue, lowMark, taskId, remoteClientsUnderFlowCtrl));
	        }
	    } else {
	    	queue.publish(message);
	    }
    }

    /**
     * enqueue a received message
     * 
     * @param message
     * @throws InterruptedException
     */
    public void enqueue(TaskMessage message, Channel channel) {

        //lots of messages may loss, when deserializeQueue haven't finish init operation
        while (!bstartRec){
            LOG.info("check deserializeQueues have already been created");
            boolean isFinishInit = true;
            for (Integer task : workerTasks){
                if (deserializeQueues.get(task) == null){
                    isFinishInit = false;
                    JStormUtils.sleepMs(10);
                    break;
                }
            }
            if (isFinishInit){
                bstartRec = isFinishInit;
            }
        }
        short type = message.get_type();

        if (type == TaskMessage.NORMAL_MESSAGE){
            //enqueue a received message
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
                String remoteAddress = channel.getRemoteAddress().toString();
                flowCtrl(channel, remoteAddress, queue, task, message.message());
            }
        } else if (type == TaskMessage.CONTROL_MESSAGE){
            //enqueue a control message
            if (recvControlQueue == null) {
                LOG.info("Can not find the recvControlQueue. So, dropping this control message");
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
            LOG.warn("Occur unexception ", e);
            return null;
        }

    }

    /**
     * register a newly created channel
     * 
     * @param channel
     */
    protected void addChannel(Channel channel) {
        allChannels.add(channel);
    }

    /**
     * close a channel
     * 
     * @param channel
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
                        // sometimes allChannels.close() will block the exit
                        // thread
                        allChannels.close().await(1, TimeUnit.SECONDS);
                        LOG.info("Successfully close all channel");
                        factory.releaseExternalResources();
                    } catch (Exception e) {

                    }
                    allChannels = null;
                }
            }).start();

            JStormUtils.sleepMs(1 * 1000);
        }
        LOG.info("Successfully shutdown NettyServer");
    }

    @Override
    public void send(List<TaskMessage> messages) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public void send(TaskMessage message) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean available() {
        return true;
    }

    public StormChannelGroup getChannelGroup() {
        return allChannels;
    }
}
