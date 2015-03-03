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
package com.alibaba.jstorm.utils;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.Metrics;
import com.codahale.metrics.Timer;
import com.lmax.disruptor.EventHandler;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * 
 * Disruptor Consumer thread
 * 
 * @author yannian
 * 
 */
public abstract class DisruptorRunable extends RunnableCallback implements EventHandler {
	private final static Logger LOG = Logger.getLogger(DisruptorRunable.class);

	protected DisruptorQueue queue;
	protected String        idStr;
	protected AtomicBoolean active;
	protected JStormTimer   timer;


	public DisruptorRunable(DisruptorQueue queue, JStormTimer timer, String idStr,
			AtomicBoolean active) {
		this.queue  = queue;
		this.timer  = timer;
		this.idStr  = idStr;
		this.active = active;
	}

	public abstract void handleEvent(Object event, boolean endOfBatch) throws Exception;
	/**
	 * This function need to be implements
	 * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long, boolean)
	 */
	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch) 
			throws Exception{
		if (event == null) {
			return ;
		}
		timer.start();
		try {
			handleEvent(event, endOfBatch);
		}finally {
			timer.stop();
		}
	}

	@Override
	public void run() {
		LOG.info("Successfully start thread " + idStr);
		queue.consumerStarted();

		while (active.get()) {
			try {

				queue.consumeBatchWhenAvailable(this);

			} catch (Exception e) {
				if (active.get() == true) {
					LOG.error("DrainerRunable send error", e);
					throw new RuntimeException(e);
				}
			}
		}
		
		LOG.info("Successfully exit thread " + idStr);
	}

	@Override
	public Object getResult() {
		if (active.get())
			return 0;
		else
			return -1;
	}

}
