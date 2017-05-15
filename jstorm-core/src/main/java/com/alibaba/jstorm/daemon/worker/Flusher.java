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

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public abstract class Flusher implements Runnable {
    protected long flushIntervalMs;
    private static FlusherPool FLUSHER;

    public static void setFlusherPool(FlusherPool flusherPool) {
        FLUSHER = flusherPool;
    }

    public abstract void run();

    public void start() {
        FLUSHER.start(this, flushIntervalMs);
    }

    public void close() {
        FLUSHER.stop(this, flushIntervalMs);
    }
}