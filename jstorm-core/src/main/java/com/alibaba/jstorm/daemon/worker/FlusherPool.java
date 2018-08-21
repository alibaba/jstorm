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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class FlusherPool {
    private final Timer timer = new Timer("flush-trigger", true);
    private final ThreadPoolExecutor threadPool;
    private final HashMap<Long, ArrayList<Flusher>> pendingFlushMap = new HashMap<>();
    private final HashMap<Long, TimerTask> timerTaskMap = new HashMap<>();

    public FlusherPool(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
        this.threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                new ArrayBlockingQueue<Runnable>(1024), new ThreadPoolExecutor.DiscardPolicy());
    }

    public synchronized void start(Flusher flusher, final long flushInterval) {
        ArrayList<Flusher> pending = pendingFlushMap.get(flushInterval);
        if (pending == null) {
            pending = new ArrayList<>();
            TimerTask t = new TimerTask() {
                @Override
                public void run() {
                    invokeAll(flushInterval);
                }
            };
            pendingFlushMap.put(flushInterval, pending);
            timer.schedule(t, flushInterval, flushInterval);
            timerTaskMap.put(flushInterval, t);
        }
        pending.add(flusher);
    }

    private synchronized void invokeAll(long flushInterval) {
        ArrayList<Flusher> tasks = pendingFlushMap.get(flushInterval);
        if (tasks != null) {
            for (Flusher f : tasks) {
                threadPool.submit(f);
            }
        }
    }

    public synchronized void stop(Flusher flusher, long flushInterval) {
        ArrayList<Flusher> pending = pendingFlushMap.get(flushInterval);
        pending.remove(flusher);
        if (pending.size() == 0) {
            pendingFlushMap.remove(flushInterval);
            timerTaskMap.remove(flushInterval).cancel();
        }
    }

    public Future<?> submit(Runnable runnable) {
        return threadPool.submit(runnable);
    }

    public void shutdown() {
        this.threadPool.shutdown();
        this.timer.cancel();
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        this.threadPool.awaitTermination(timeout, unit);
    }

}