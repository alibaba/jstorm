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
package com.alibaba.jstorm.task;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Sending entrance
 * 
 * Task sending all tuples through this Object
 * 
 * Serialize the Tuple and put the serialized data to the sending queue
 * 
 * @author yannian
 * 
 */
public class TaskTransfer {

	private static Logger LOG = Logger.getLogger(TaskTransfer.class);

	private Map storm_conf;
	private DisruptorQueue transferQueue;
	private KryoTupleSerializer serializer;
	private Map<Integer, DisruptorQueue> innerTaskTransfer;
	private DisruptorQueue serializeQueue;
	private final AsyncLoopThread serializeThread;
	private volatile TaskStatus taskStatus;
	private String taskName;
	private JStormTimer  timer;

	public TaskTransfer(String taskName, 
			KryoTupleSerializer serializer, TaskStatus taskStatus,
			WorkerData workerData) {
		this.taskName = taskName;
		this.serializer = serializer;
		this.taskStatus = taskStatus;
		this.storm_conf = workerData.getConf();
		this.transferQueue = workerData.getTransferQueue();
		this.innerTaskTransfer = workerData.getInnerTaskTransfer();

		int queue_size = Utils.getInt(storm_conf
				.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		this.serializeQueue = DisruptorQueue.mkInstance(taskName, ProducerType.MULTI, 
				queue_size, waitStrategy);
		this.serializeQueue.consumerStarted();
		
		String taskId = taskName.substring(taskName.indexOf(":") + 1);
		Metrics.registerQueue(taskName, MetricDef.SERIALIZE_QUEUE, serializeQueue, taskId, Metrics.MetricType.TASK);
		timer = Metrics.registerTimer(taskName, MetricDef.SERIALIZE_TIME, taskId, Metrics.MetricType.TASK); 

		serializeThread = new AsyncLoopThread(new TransferRunnable());
		LOG.info("Successfully start TaskTransfer thread");

	}

	public void transfer(TupleExt tuple) {

		int taskid = tuple.getTargetTaskId();

		DisruptorQueue exeQueue = innerTaskTransfer.get(taskid);
		if (exeQueue != null) {
			exeQueue.publish(tuple);
		} else {
			serializeQueue.publish(tuple);
		}

	}

	public AsyncLoopThread getSerializeThread() {
		return serializeThread;
	}

	class TransferRunnable extends RunnableCallback implements EventHandler {
		
		@Override
		public String getThreadName() {
			return taskName + "-" +TransferRunnable.class.getSimpleName();
		}

		@Override
		public void run() {

			WorkerClassLoader.switchThreadContext();
			while (taskStatus.isShutdown() == false) {
				serializeQueue.consumeBatchWhenAvailable(this);

			}
			WorkerClassLoader.restoreThreadContext();
		}

		public Object getResult() {
			if (taskStatus.isShutdown() == false) {
				return 0;
			} else {
				return -1;
			}
		}

		@Override
		public void onEvent(Object event, long sequence, boolean endOfBatch)
				throws Exception {

			if (event == null) {
				return;
			}
			
			timer.start();

			try {
				TupleExt tuple = (TupleExt) event;
				int taskid = tuple.getTargetTaskId();
				byte[] tupleMessage = serializer.serialize(tuple);
				TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
				transferQueue.publish(taskMessage);
			}finally {
				timer.stop();
			}
			

		}

	}

}
