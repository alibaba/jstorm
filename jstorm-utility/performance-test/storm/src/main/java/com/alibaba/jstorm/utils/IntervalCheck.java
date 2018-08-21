package com.alibaba.jstorm.utils;

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

import java.io.Serializable;

public class IntervalCheck implements Serializable {
    private static final long serialVersionUID = 8952971673547362883L;

    long lastCheck = 0;

    // default interval is 1 second
    long interval = 1000;

    /*
     * if last check time is before interval seconds, return true, otherwise return false
     */
    public boolean check() {
        return checkAndGet() != null;
    }

    public Double checkAndGet() {
        long now = System.currentTimeMillis();

        synchronized (this) {
            if (now >= interval + lastCheck) {
                double pastSecond = ((double) (now - lastCheck)) / 1000;
                lastCheck = now;
                return pastSecond;
            }
        }

        return null;
    }

    public long getInterval() {
        return interval / 1000;
    }

    public long getIntervalMs() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval * 1000;
    }

    public void setIntervalMs(long interval) {
        this.interval = interval;
    }

    public void adjust(long addTimeMillis) {
        lastCheck += addTimeMillis;
    }

    public boolean isStart() {
        return lastCheck != 0;
    }

    public void start() {
        lastCheck = System.currentTimeMillis();
    }

    public long getLaskCheckTime() {
        return lastCheck;
    }
}

