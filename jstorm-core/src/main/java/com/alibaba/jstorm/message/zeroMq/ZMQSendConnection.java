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
package com.alibaba.jstorm.message.zeroMq;

import java.util.List;

import org.zeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.JStormHistogram;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.utils.JStormServerUtils;

/**
 * 
 * @author longda
 * 
 */
public class ZMQSendConnection implements IConnection {
	private org.zeromq.ZMQ.Socket socket;
	private boolean closed = false;
	private JStormTimer timer;
	private JStormHistogram histogram;
	private String prefix;

	public ZMQSendConnection(Socket _socket, String host, int port) {
		socket = _socket;
		prefix = JStormServerUtils.getName(host, port);
		timer = Metrics.registerTimer(prefix, MetricDef.ZMQ_SEND_TIME, 
				null, Metrics.MetricType.WORKER);
		histogram = Metrics.registerHistograms(prefix, MetricDef.ZMQ_SEND_MSG_SIZE, 
				null, Metrics.MetricType.WORKER);
	}

	@Override
	public void close() {
		socket.close();
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public void registerQueue(DisruptorQueue recvQueu) {
		throw new UnsupportedOperationException(
				"recvTask() Client connection should not receive any messages");
	}

	@Override
	public void enqueue(TaskMessage message) {
		throw new UnsupportedOperationException(
				"recvTask() Client connection should not receive any messages");
	}

	@Override
	public void send(List<TaskMessage> messages) {
		timer.start();

		try {
			for (TaskMessage message : messages) {
				ZeroMq.send(socket, message.message());
			}
		} finally {
			timer.stop();
			histogram.update(messages.size());
		}
	}

	@Override
	public void send(TaskMessage message) {
		timer.start();
		try {
			ZeroMq.send(socket, message.message());
		} finally {
			timer.stop();
			histogram.update(1);
		}
	}

	@Override
	public TaskMessage recv(int flags) {
		throw new UnsupportedOperationException(
				"recvTask() Client connection should not receive any messages");
	}

}
