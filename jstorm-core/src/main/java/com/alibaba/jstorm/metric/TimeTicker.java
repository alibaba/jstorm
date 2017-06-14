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

import java.util.concurrent.TimeUnit;

/**
 * a simple util class to calculate run time
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class TimeTicker {
    private TimeUnit unit;
    private long start;
    private long end;

    public TimeTicker(TimeUnit unit) {
        if (unit != TimeUnit.NANOSECONDS && unit != TimeUnit.MILLISECONDS) {
            throw new IllegalArgumentException("invalid unit!");
        }
        this.unit = unit;
    }

    public TimeTicker(TimeUnit unit, boolean start) {
        this(unit);
        if (start) {
            start();
        }
    }

    public void start() {
        if (unit == TimeUnit.MILLISECONDS) {
            this.start = System.currentTimeMillis();
        } else if (unit == TimeUnit.NANOSECONDS) {
            this.start = System.nanoTime();
        }
    }

    public long stop() {
        if (unit == TimeUnit.MILLISECONDS) {
            this.end = System.currentTimeMillis();
        } else if (unit == TimeUnit.NANOSECONDS) {
            this.end = System.nanoTime();
        }
        return end - start;
    }

    public long stopAndRestart() {
        long elapsed = stop();
        start();
        return elapsed;
    }
}
