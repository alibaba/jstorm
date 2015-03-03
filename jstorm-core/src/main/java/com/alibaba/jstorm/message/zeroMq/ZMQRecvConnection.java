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

import org.apache.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.utils.DisruptorQueue;

/**
 * 
 * @author longda
 * 
 */
public class ZMQRecvConnection extends RunnableCallback implements IConnection {
	private static final Logger LOG = Logger.getLogger(ZMQRecvConnection.class);
	
	private Socket socket;
	private boolean closed = false;
	private DisruptorQueue recvQueue;

	public ZMQRecvConnection(Socket _socket) {
		socket = _socket;
		
		new AsyncLoopThread(this, true,
				Thread.MAX_PRIORITY, true);
	}

	@Override
	public TaskMessage recv(int flags) {
		byte[] data =  ZeroMq.recv(socket, flags);
		if (data == null || data.length <= 4) {
			return null;
		}
		int port = KryoTupleDeserializer.deserializeTaskId(data);
		return new TaskMessage(port, data);
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
	public void send(List<TaskMessage> messages) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
	}
	@Override
	public void send(TaskMessage message) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
	}

	@Override
	public void registerQueue(DisruptorQueue recvQueu) {
		this.recvQueue = recvQueu;
	}

	@Override
	public void enqueue(TaskMessage message) {
		if (message != null ) {
			recvQueue.publish(message);
		}
		
	}

	@Override
	public void run() {
		LOG.info("Successfully start ZMQ Recv thread");
		
		while(isClosed() == false) {
			try {
				TaskMessage message = recv(0);
				enqueue(message);
			}catch (Exception e) {
				LOG.warn("ZMQ Recv thread receive error", e);
			}
		}
		
		LOG.info("Successfully shutdown ZMQ Recv thread");
		
	}

	@Override
	public Object getResult() {
		LOG.info("Begin to shutdown ZMQ Recv thread");
		return -1;
	}

	
}
