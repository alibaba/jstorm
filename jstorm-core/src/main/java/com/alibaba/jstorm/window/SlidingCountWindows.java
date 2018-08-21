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
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;

/**
 * A {@link WindowAssigner} that assigns elements to windows based on the global offset of elements.
 */
public class SlidingCountWindows<T> extends WindowAssigner<T> {
    private static final long serialVersionUID = 1L;

    private final long size;
    private final long slide;

    private long offset = -1;

    private SlidingCountWindows(long size, long slide) {
        this.size = size;
        this.slide = slide;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        offset++;

        long start = offset / slide * slide - slide;
        if (start < 0) {
            start = 0;
        }

        List<TimeWindow> windows = Lists.newArrayList();
        for (long nextStart = start; offset >= nextStart; nextStart += slide) {
            if (offset < nextStart + size) {
                windows.add(new TimeWindow(nextStart, nextStart + size));
            }
        }
        return windows;
    }

    @Override
    public Trigger<T> getDefaultTrigger(TopologyContext topologyContext) {
        return CountTrigger.of(this.size);
    }

    @Override
    public String toString() {
        return "GlobalWindows()";
    }

    /**
     * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns
     * all elements to the same {@link GlobalWindow}.
     *
     * @return The global window policy.
     */
    public static <T> SlidingCountWindows<T> create(long size, long slide) {
        return new SlidingCountWindows<>(size, slide);
    }

    public static void main(String[] args) {
        // o o o o o o o o o o
        // 0 1 2 3 4 5 6 7 8 9
        SlidingCountWindows<Tuple> slideCountAssigner = SlidingCountWindows.create(3, 1);
        for (int i = 0; i < 10; i++) {
            System.out.println("i=" + i);
            Collection<TimeWindow> windows = slideCountAssigner.assignWindows(new TupleImpl(), 0);
            for (TimeWindow window : windows) {
                System.out.println(window);
            }
            System.out.println();
        }
    }
}
