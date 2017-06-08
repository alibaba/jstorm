/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.window;

import backtype.storm.task.TopologyContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link WindowAssigner} that windows elements into sliding windows based on the timestamp of the
 * elements. Windows can possibly overlap.
 */
public class SlidingEventTimeWindows<T> extends WindowAssigner<T> {
    private static final long serialVersionUID = 1L;

    private final long size;
    private final long slide;

    protected SlidingEventTimeWindows(long size, long slide) {
        this.size = size;
        this.slide = slide;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        if (timestamp > Long.MIN_VALUE) {
            List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
            long lastStart = timestamp - timestamp % slide;
            for (long start = lastStart;
                 start > timestamp - size;
                 start -= slide) {
                windows.add(new TimeWindow(start, start + size));
            }
            return windows;
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
                    "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
                    "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public Trigger<T> getDefaultTrigger(TopologyContext context) {
        return EventTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "SlidingEventTimeWindows(" + size + ", " + slide + ")";
    }

    /**
     * Creates a new {@code SlidingEventTimeWindows} {@link WindowAssigner} that assigns
     * elements to sliding time windows based on the element timestamp.
     *
     * @param size  The size of the generated windows.
     * @param slide The slide interval of the generated windows.
     * @return The time policy.
     */
    public static <T> SlidingEventTimeWindows<T> of(long size, long slide) {
        return new SlidingEventTimeWindows<>(size, slide);
    }
}
