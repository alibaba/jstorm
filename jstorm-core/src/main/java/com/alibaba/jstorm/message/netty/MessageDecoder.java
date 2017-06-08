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

import backtype.storm.messaging.ControlMessage;
import backtype.storm.messaging.TaskMessage;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDecoder extends FrameDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(MessageDecoder.class);

    /**
     * netty decode timer
     */
    private static AsmHistogram msgDecodeTimer;

    /**
     * netty transmit time map
     */
    private static Map<Channel, AsmHistogram> networkTransmitTimeMap = new HashMap<>();

    private static Map<Channel, String> transmitNameMap = new HashMap<>();
    private boolean isServer;
    private String localIp;
    private int localPort;

    private boolean enableNettyMetrics;

    public MessageDecoder(boolean isServer, Map conf) {
        this.isServer = isServer;
        this.localPort = ConfigExtension.getLocalWorkerPort(conf);
        this.localIp = NetWorkUtils.ip();
        this.enableNettyMetrics = MetricUtils.isEnableNettyMetrics(conf);

        if (enableNettyMetrics) {
            // this metric will lose roughly 2~3% performance
            msgDecodeTimer = (AsmHistogram) JStormMetrics.registerWorkerMetric(
                    MetricUtils.workerMetricName(MetricDef.NETWORK_MSG_DECODE_TIME, MetricType.HISTOGRAM), new AsmHistogram());
            msgDecodeTimer.setTimeUnit(TimeUnit.NANOSECONDS);
        }
        NettyMetricInstance.register();
    }

    /*
     * Each ControlMessage is encoded as: code (<0) ... short(2)
     * Each TaskMessage is encoded as: task (>=0) ... short(2) len ... int(4) payload ... byte[] *
     */
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Make sure that we have received at least a short
        long available = buf.readableBytes();
        // Length of control message is 10.
        // Minimum length of a task message is 6(short taskId, int length).
        if (available < 6) {
            // need more data
            return null;
        }

        long startTime = 0L;
        if (isServer && enableNettyMetrics) {
            startTime = msgDecodeTimer.getTime();
        }
        try {
            // Mark the current buffer position before reading task/len field
            // because the whole frame might not be in the buffer yet.
            // We will reset the buffer position to the marked position if
            // there's not enough bytes in the buffer.
            buf.markReaderIndex();

            // read the short field
            short code = buf.readShort();
            available -= 2;

            // case 1: Control message
            ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
            if (ctrl_msg != null) {
                if (available < 12) {
                    // The time stamp bytes were not received yet - return null.
                    buf.resetReaderIndex();
                    return null;
                }
                long timeStamp = buf.readLong();
                int clientPort = buf.readInt();
                available -= 12;
                if (ctrl_msg == ControlMessage.EOB_MESSAGE) {
                    long interval = System.currentTimeMillis() - timeStamp;
                    if (interval < 0)
                        interval = 0;

                    if (enableNettyMetrics) {
                        AsmHistogram netTransTime = getTransmitHistogram(channel, clientPort);
                        if (netTransTime != null) {
                            netTransTime.update(interval * TimeUtils.NS_PER_US);
                        }
                    }
                    if (MetricUtils.metricAccurateCal)
                        NettyMetricInstance.networkWorkerTransmitTime.update(interval * TimeUtils.NS_PER_US);
                }
                NettyMetricInstance.nettyServerRecvSpeed.update(ctrl_msg.getEncodedLength());
                return ctrl_msg;
            }

            // case 2: task Message
            short type = code;
            short task = buf.readShort();
            available -= 2;

            // Make sure that we have received at least an integer (length)
            if (available < 4) {
                // need more data
                buf.resetReaderIndex();

                return null;
            }

            // Read the length field.
            int length = buf.readInt();
            if (length <= 0) {
                LOG.info("Receive one message whose TaskMessage's message length is {}", length);
                return new TaskMessage(task, null);
            }

            // Make sure if there's enough bytes in the buffer.
            available -= 4;
            if (available < length) {
                // The whole bytes were not received yet - return null.
                buf.resetReaderIndex();

                return null;
            }

            // There's enough bytes in the buffer. Read it.
            ChannelBuffer payload = buf.readBytes(length);

            // Successfully decoded a frame.
            // Return a TaskMessage object

            byte[] rawBytes = payload.array();
            // @@@ TESTING CODE
            // LOG.info("Receive task:{}, length: {}, data:{}",
            // task, length, JStormUtils.toPrintableString(rawBytes));

            TaskMessage ret = new TaskMessage(type, task, rawBytes);
            NettyMetricInstance.nettyServerRecvSpeed.update(rawBytes.length + 8);
            return ret;
        } finally {
            if (isServer && enableNettyMetrics && msgDecodeTimer != null) {
                msgDecodeTimer.updateTime(startTime);
            }
        }

    }

    public AsmHistogram getTransmitHistogram(Channel channel, int clientPort) {
        AsmHistogram netTransTime = networkTransmitTimeMap.get(channel);
        if (netTransTime == null) {
            InetSocketAddress sockAddr = (InetSocketAddress) (channel.getRemoteAddress());

            String nettyConnection = NettyConnection.mkString(sockAddr.getAddress().getHostAddress(), clientPort, localIp, localPort);
            netTransTime = (AsmHistogram) JStormMetrics.registerNettyMetric(
                    MetricUtils.nettyMetricName(AsmMetric.mkName(MetricDef.NETTY_SRV_MSG_TRANS_TIME, nettyConnection), MetricType.HISTOGRAM),
                    new AsmHistogram());

            networkTransmitTimeMap.put(channel, netTransTime);
            transmitNameMap.put(channel, nettyConnection);
            LOG.info("Register transmit histogram of {}, channel {}", nettyConnection, channel);
        }

        return netTransTime;
    }

    public static void removeTransmitHistogram(Channel channel) {
        AsmHistogram netTransTime = networkTransmitTimeMap.remove(channel);
        if (netTransTime != null) {
            String nettyConnection = transmitNameMap.remove(channel);
            JStormMetrics.unregisterNettyMetric(MetricUtils.nettyMetricName(AsmMetric.mkName(MetricDef.NETTY_SRV_MSG_TRANS_TIME, nettyConnection),
                    MetricType.HISTOGRAM));
            LOG.info("Remove transmit histogram of {}, channel {}", nettyConnection, channel);
        }
    }
}