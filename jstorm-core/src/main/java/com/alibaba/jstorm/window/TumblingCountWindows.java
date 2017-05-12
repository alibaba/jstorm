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

/**
 * A {@link WindowAssigner} that assigns all elements to the same global window.
 */
public class TumblingCountWindows<T> extends WindowAssigner<T> {
    private static final long serialVersionUID = 1L;

    private final long size;

    private long offset = -1;

    private TumblingCountWindows(long size) {
        this.size = size;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        offset++;

        long start = offset / size * size;
        return Lists.newArrayList(new TimeWindow(start, start + size));
    }

    @Override
    public Trigger<T> getDefaultTrigger(TopologyContext topologyContext) {
        return CountTrigger.of(size);
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
    public static <T> TumblingCountWindows<T> create(long size) {
        return new TumblingCountWindows<>(size);
    }

    public static void main(String[] args) {
        // o o o o o o o o o o
        // 0 1 2 3 4 5 6 7 8 9
        TumblingCountWindows<Tuple> slideCountAssigner = TumblingCountWindows.create(3);
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
