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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.utils.DisruptorRunable;
import com.alibaba.jstorm.utils.Pair;
import com.lmax.disruptor.EventHandler;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * 
 * Tuple sender
 * 
 * @author yannian
 * 
 */
public class BatchTupleRunable extends DisruptorRunable {
	private final static Logger LOG = Logger.getLogger(BatchTupleRunable.class);

	private DisruptorQueue transferQueue;
	private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
	private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

	
	private Map<IConnection, List<TaskMessage>> dispatchMap;
	private DisruptorQueue sendingQueue;
	
	private final boolean isDirectSend = true;
	
	private static JStormTimer timer = Metrics.registerTimer(null, 
			MetricDef.BATCH_TUPLE_TIME, null, Metrics.MetricType.WORKER);
	private DisruptorQueue queue;

	public BatchTupleRunable(WorkerData workerData) {
		super(workerData.getTransferQueue(), timer, BatchTupleRunable.class.getSimpleName(), workerData.getActive());
		this.sendingQueue = workerData.getSendingQueue();
		this.nodeportSocket = workerData.getNodeportSocket();
		this.taskNodeport = workerData.getTaskNodeport();
		this.dispatchMap = new HashMap<IConnection, List<TaskMessage>>();
		
		this.queue = workerData.getTransferQueue();
		Metrics.registerQueue(null, MetricDef.BATCH_TUPLE_QUEUE, this.queue, null, Metrics.MetricType.WORKER);
		
		this.queue.consumerStarted();
	}
	
	public void handleOneEvent(TaskMessage felem) {

		int taskId = felem.task();
		byte[] tuple = felem.message();

		WorkerSlot nodePort = taskNodeport.get(taskId);
		if (nodePort == null) {
			String errormsg = "can`t not found IConnection to " + taskId;
			LOG.warn("DrainerRunable warn", new Exception(errormsg));
			return;
		}
		IConnection conn = nodeportSocket.get(nodePort);
		if (conn == null) {
			String errormsg = "can`t not found nodePort " + nodePort;
			LOG.warn("DrainerRunable warn", new Exception(errormsg));
			return;
		}

		if (conn.isClosed() == true) {
			// if connection has been closed, just skip the package
			return;
		}
		
		if (isDirectSend) {
			conn.send(felem);
			return ;
		}

		List<TaskMessage> list = dispatchMap.get(conn);
		if (list == null) {
			list = new ArrayList<TaskMessage>();
			dispatchMap.put(conn, list);
		}
		list.add(felem);
		return ;

	}
	
	public void handleFinish() {
		for (Entry<IConnection, List<TaskMessage>> entry: dispatchMap.entrySet()) {
			Pair<IConnection, List<TaskMessage>> pair = 
					new Pair<IConnection, List<TaskMessage>>(
							entry.getKey(), entry.getValue());
			
			sendingQueue.publish(pair);
		}
		
		dispatchMap.clear();
	}

	@Override
	public void handleEvent(Object event, boolean endOfBatch)
			throws Exception {
	
		handleOneEvent((TaskMessage)event);
		
		if (endOfBatch == true && isDirectSend == false) {
			handleFinish();
		}
		
	}

}
