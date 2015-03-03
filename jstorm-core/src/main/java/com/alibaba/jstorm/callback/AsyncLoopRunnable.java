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
package com.alibaba.jstorm.callback;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * AsyncLoopThread 's runnable
 * 
 * The class wrapper RunnableCallback fn, if occur exception, run killfn
 * 
 * @author yannian
 * 
 */
public class AsyncLoopRunnable implements Runnable {
	private static Logger LOG = Logger.getLogger(AsyncLoopRunnable.class);

	private RunnableCallback fn;
	private RunnableCallback killfn;

	public AsyncLoopRunnable(RunnableCallback fn, RunnableCallback killfn) {
		this.fn = fn;
		this.killfn = killfn;
	}

	private boolean needQuit(Object rtn) {
		if (rtn != null) {
			long sleepTime = Long.parseLong(String.valueOf(rtn));
			if (sleepTime < 0) {
				return true;
			}else if (sleepTime > 0) {
				JStormUtils.sleepMs(sleepTime * 1000);
			} 
		}
		return false;
	}

	@Override
	public void run() {

		try {
			while (true) {
				Exception e = null;

				try {
					if (fn == null) {
						LOG.warn("fn==null");
						throw new RuntimeException("AsyncLoopRunnable no core function ");
					}

					fn.run();

					e = fn.error();

				} catch (Exception ex) {
					e = ex;
				}
				if (e != null) {
					fn.shutdown();
					throw e;
				}
				Object rtn = fn.getResult();
				if (this.needQuit(rtn)) {
					return;
				}

			}
		} catch (InterruptedException e) {
			LOG.info("Async loop interrupted!");
		} catch (Throwable e) {
			Object rtn = fn.getResult();
			if (this.needQuit(rtn)) {
				return;
			}else {
				LOG.error("Async loop died!", e);
				killfn.execute(e);
			}
		}

	}

}
