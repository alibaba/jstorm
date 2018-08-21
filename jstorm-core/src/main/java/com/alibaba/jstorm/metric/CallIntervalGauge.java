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

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class CallIntervalGauge implements Gauge<Double> {
    private AtomicLong times = new AtomicLong(0);
    private long lastTime = System.currentTimeMillis();
    private Double value = 0.0;

    public long incrementAndGet(){
        return times.incrementAndGet();
    }
    public long addAndGet(long delta){
        return times.addAndGet(delta);
    }


    @Override
    public Double getValue() {
        long totalTimes = times.getAndSet(0);
        long now = System.currentTimeMillis();
        long timeInterval = now - lastTime;
        lastTime = now;
        if (timeInterval > 0 && totalTimes > 0){
            //convert us
            value = timeInterval/(double)totalTimes * 1000;
        }else {
            value = 0.0;
        }
        return value;
    }
}
