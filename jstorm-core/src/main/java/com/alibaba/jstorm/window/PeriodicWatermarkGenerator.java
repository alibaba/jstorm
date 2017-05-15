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
package com.alibaba.jstorm.window;

import backtype.storm.task.TopologyContext;
import java.util.Map;

/**
 * Periodic watermark generator, used in WindowedBolt when upstream/source doesn't send watermarks.
 * Use code like following to register a watermark generator:
 * <pre>{@code
 *  BaseWindowedBolt windowedBolt = new YourBolt();
 *  windowedBolt.withWatermarkGenerator(new PeriodicWatermarkGenerator());
 * }
 * </pre>
 *
 * Note that periodic watermark generator might be quite inaccurate about real watermarks since it has no context
 * about data source. A better approach would be sending watermarks from spout manually, or track upstream watermarks
 * so that you have a big picture about the progress of upstream flow.
 *
 * @author Cody
 * @since 2.3.0
 */
public class PeriodicWatermarkGenerator implements WatermarkGenerator {

    private static final long serialVersionUID = 1L;

    private long maxTimestamp = Long.MIN_VALUE;
    private long maxLagMs = 0L;
    private long watermarkInterval;

    public PeriodicWatermarkGenerator(Time maxLag, Time triggerPeriod) {
        this.maxLagMs = maxLag.toMilliseconds();
        this.watermarkInterval = triggerPeriod.toMilliseconds();
    }

    @Override
    public void init(Map conf, TopologyContext context) {
    }

    @Override
    public long getCurrentWatermark() {
        if (maxTimestamp < 0) {
            return 0L;
        }
        long ts = maxTimestamp - maxLagMs;
        if (ts < 0) {
            return 0L;
        }
        return ts;
    }

    @Override
    public void onElement(long timestamp) {
        if (timestamp > maxTimestamp) {
            this.maxTimestamp = timestamp;
        }
    }

    @Override
    public long getWatermarkInterval() {
        return this.watermarkInterval;
    }
}