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