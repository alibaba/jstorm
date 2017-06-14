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
package com.alibaba.jstorm.ons;

import com.aliyun.openservices.ons.api.Message;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OnsTuple implements Serializable {

	/**  */
	private static final long serialVersionUID = 2277714452693486955L;
	
	protected final Message message;

	protected final AtomicInteger failureTimes;
	protected final long createMs;
	protected long emitMs;

	protected transient CountDownLatch latch;
	protected transient boolean isSuccess;

	public OnsTuple(Message message) {
		this.message = message;

		this.failureTimes = new AtomicInteger(0);
		this.createMs = System.currentTimeMillis();

		this.latch = new CountDownLatch(1);
		this.isSuccess = false;
	}

	public AtomicInteger getFailureTimes() {
		return failureTimes;
	}
	
	public long getCreateMs() {
		return createMs;
	}

	public long getEmitMs() {
		return emitMs;
	}

	public void updateEmitMs() {
		this.emitMs = System.currentTimeMillis();
	}

	public Message getMessage() {
		return message;
	}

	public boolean waitFinish() throws InterruptedException {
		return latch.await(4, TimeUnit.HOURS);
	}

	public void done() {
		isSuccess = true;
		latch.countDown();
	}

	public void fail() {
		isSuccess = false;
		latch.countDown();
	}

	public boolean isSuccess() {
		return isSuccess;
	}
	

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
