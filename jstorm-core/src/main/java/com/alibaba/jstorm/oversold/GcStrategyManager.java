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
package com.alibaba.jstorm.oversold;

import com.alibaba.jstorm.client.ConfigExtension;

import java.util.Map;

/**
 * @author fengjian
 * @since 16/11/14
 */
public class GcStrategyManager {


    public static GcStrategy getGcStrategy(Map conf) {
        double memWeight = ConfigExtension.getSupervisorSlotsPortMemWeight(conf);
        if (memWeight >= 1) memWeight = 1;
        else if (memWeight <= 0.5) memWeight = 0.5;
        GcStrategy gcStrategy = new GcStrategy();
        gcStrategy.setGcType(GC.ConcMarkSweepGC);
        gcStrategy.setInitiatingOccupancyFraction(memWeight * 0.68);
        gcStrategy.setUseInitiatingOccupancyOnly(true);
        gcStrategy.setSurvivorRatio(4);
        return gcStrategy;
    }
}
