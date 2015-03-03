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
package com.alibaba.jstorm.daemon.worker;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.utils.DisruptorRunable;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * Message dispatcher
 * 
 * @author yannian/Longda
 * 
 */
public class VirtualPortDispatch extends DisruptorRunable {
	private final static Logger LOG = Logger
			.getLogger(VirtualPortDispatch.class);

	private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;
	private IConnection recvConnection;

	private static JStormTimer timer = Metrics.registerTimer(null, 
			MetricDef.DISPATCH_TIME, null, Metrics.MetricType.WORKER);
	
	public VirtualPortDispatch(WorkerData workerData,
			IConnection recvConnection, DisruptorQueue recvQueue) {
		super(recvQueue, timer, VirtualPortDispatch.class.getSimpleName(), 
				workerData.getActive());

		this.recvConnection = recvConnection;
		this.deserializeQueues = workerData.getDeserializeQueues();

		Metrics.registerQueue(null, MetricDef.DISPATCH_QUEUE, queue, null, Metrics.MetricType.WORKER);
	}

	public void cleanup() {
		LOG.info("Begin to shutdown VirtualPortDispatch");
		// don't need send shutdown command to every task
		// due to every task has been shutdown by workerData.active
		// at the same time queue has been fulll
//		byte shutdownCmd[] = { TaskStatus.SHUTDOWN };
//		for (DisruptorQueue queue : deserializeQueues.values()) {
//
//			queue.publish(shutdownCmd);
//		}

		try {
			recvConnection.close();
		}catch(Exception e) {
			
		}
		recvConnection = null;
		Metrics.unregister(null, MetricDef.DISPATCH_QUEUE,  null, Metrics.MetricType.WORKER);
		LOG.info("Successfully shudown VirtualPortDispatch");
	}

	@Override
	public void handleEvent(Object event, boolean endOfBatch)
			throws Exception {
		TaskMessage message = (TaskMessage) event;

		int task = message.task();

		DisruptorQueue queue = deserializeQueues.get(task);
		if (queue == null) {
			LOG.warn("Received invalid message directed at port " + task
					+ ". Dropping...");
			return;
		}

		queue.publish(message.message());

	}

}
