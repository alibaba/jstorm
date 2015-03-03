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
package com.alibaba.jstorm.metric;


import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.codahale.metrics.Timer;

public class JStormTimer {
	private static final Logger LOG = Logger.getLogger(JStormTimer.class);
	private static boolean isEnable = true;

	public static boolean isEnable() {
		return isEnable;
	}

	public static void setEnable(boolean isEnable) {
		JStormTimer.isEnable = isEnable;
	}
	
	
	private Timer  instance;
	private String name;
	public JStormTimer(String name, Timer instance) {
		this.name = name;
		this.instance = instance;
		this.timerContext = new AtomicReference<Timer.Context>();
	}
	
	/**
	 * This logic isn't perfect, it will miss metrics when it is called 
	 * in the same time. But this method performance is better than 
	 * create a new instance wrapper Timer.Context
	 */
	private AtomicReference<Timer.Context> timerContext = null;
	public void start() {
		if (JStormTimer.isEnable == false) {
			return ;
		}
		
		if (timerContext.compareAndSet(null, instance.time()) == false) {
			LOG.warn("Already start timer " + name);
			return ;
		}
		
	}
	
	public void stop() {
		Timer.Context context = timerContext.getAndSet(null);
		if (context != null) {
			context.stop();
		}
	}

	public Timer getInstance() {
		return instance;
	}
	
	
}
