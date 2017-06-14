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
package com.dianping.cosmos;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dp.blackhole.consumer.MessageStream;

public class MessageFetcher implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(MessageFetcher.class);
    private final int MAX_QUEUE_SIZE = 1000;
    private final int TIME_OUT = 5000;

    private BlockingQueue<String> emitQueue;
    private MessageStream stream;

    private volatile boolean running;
    public MessageFetcher(MessageStream stream) {
        this.running = true;
        this.stream = stream;
        this.emitQueue = new LinkedBlockingQueue<String>(MAX_QUEUE_SIZE);
    }
    
    @Override
    public void run() {
        while (running) {
            for (String message : stream) {
                try {
                    while(!emitQueue.offer(message, TIME_OUT, TimeUnit.MILLISECONDS)) {
                        LOG.error("Queue is full, cannot offer message.");
                    }
                } catch (InterruptedException e) {
                    LOG.error("Thread Interrupted");
                    running = false;
                }
            }
        }
    }
    
    public String pollMessage() {
        return emitQueue.poll();
    }
    
    public void shutdown() {
        this.running = false;
    }
}
