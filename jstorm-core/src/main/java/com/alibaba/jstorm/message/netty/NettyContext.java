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
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyContext implements IContext {
    private final static Logger LOG = LoggerFactory.getLogger(NettyContext.class);
    @SuppressWarnings("rawtypes")
    private Map stormConf;
    private NioClientSocketChannelFactory clientChannelFactory;
    private ReconnectRunnable reconnector;

    @SuppressWarnings("unused")
    public NettyContext() {
    }

    /**
     * initialization per Storm configuration
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf) {
        this.stormConf = stormConf;

        int maxWorkers = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));
        ThreadFactory bossFactory = new NettyRenameThreadFactory(MetricDef.NETTY_CLI + "boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory(MetricDef.NETTY_CLI + "worker");

        if (maxWorkers > 0) {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory));
        }

        reconnector = new ReconnectRunnable();
        new AsyncLoopThread(reconnector, true, Thread.MIN_PRIORITY, true);
    }

    @Override
    public IConnection bind(String topologyId, int port, ConcurrentHashMap<Integer, DisruptorQueue> deserializedQueue,
                            DisruptorQueue recvControlQueue, boolean bstartRec, Set<Integer> workerTasks) {
        IConnection retConnection = null;
        try {
            retConnection = new NettyServer(stormConf, port, deserializedQueue, recvControlQueue, bstartRec, workerTasks);
        } catch (Throwable e) {
            LOG.error("Failed to create NettyServer", e);
            JStormUtils.halt_process(-1, "Failed to bind " + port);
        }

        return retConnection;
    }

    @Override
    public IConnection connect(String topologyId, String host, int port) {
        return new NettyClientAsync(stormConf, clientChannelFactory, host, port, reconnector, new HashSet<Integer>(), new HashSet<Integer>());
    }

    @Override
    public IConnection connect(String topologyId, String host, int port, Set<Integer> sourceTasks, Set<Integer> targetTasks) {
        return new NettyClientAsync(stormConf, clientChannelFactory, host, port, reconnector, sourceTasks, targetTasks);
    }

    /**
     * terminate this context
     */
    public void term() {
/*        clientScheduleService.shutdown();
        try {
            clientScheduleService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error when shutting down client scheduler", e);
        }*/

        clientChannelFactory.releaseExternalResources();

        reconnector.shutdown();
    }

}
