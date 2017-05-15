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

import backtype.storm.GCState;
import com.codahale.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class FullGcGauge implements Gauge<Double> {

    private static final Logger LOG = LoggerFactory.getLogger(FullGcGauge.class);

    private double lastFGcNum = 0.0;


    @Override
    public Double getValue() {
        double newFGcNum = GCState.getOldGenCollectionCount();
        double delta = newFGcNum - lastFGcNum;
        if (delta < 0){
            LOG.warn("new Fgc {} little than old oldFgc {}", newFGcNum, lastFGcNum);
            delta = 0;
        }
        lastFGcNum = newFGcNum;
        return delta;
    }
}
