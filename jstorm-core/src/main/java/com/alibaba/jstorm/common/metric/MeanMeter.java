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
package com.alibaba.jstorm.common.metric;

import com.codahale.metrics.Metered;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wange
 * @since 2.1.1
 */
public class MeanMeter implements Metered {

    private final AtomicLong count = new AtomicLong(0L);
    private final long startTime;

    /**
     * Creates a new {@link MeanMeter}.
     */
    public MeanMeter() {
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        count.addAndGet(n);
    }

    @Override
    public long getCount() {
        return count.get();
    }

    @Override
    public double getFifteenMinuteRate() {
        return 0d;
    }

    @Override
    public double getFiveMinuteRate() {
        return 0d;
    }

    @Override
    public double getMeanRate() {
        return calcMean();
    }

    @Override
    public double getOneMinuteRate() {
        return calcMean();
    }

    private double calcMean() {
        double elapsedSec = (System.currentTimeMillis() - startTime) / 1000d;
        if (elapsedSec > 0) {
            return count.get() / elapsedSec;
        }
        return 0;
    }
}
