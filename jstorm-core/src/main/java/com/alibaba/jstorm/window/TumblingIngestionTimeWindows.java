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
import backtype.storm.tuple.TupleExt;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into windows based on the current
 * system time of the machine the operation is running on. Windows cannot overlap.
 */
public class TumblingIngestionTimeWindows<T> extends WindowAssigner<T> {
    private static final long serialVersionUID = 1L;

    private long size;

    private TumblingIngestionTimeWindows(long size) {
        this.size = size;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        long now = ((TupleExt) element).getCreationTimeStamp();
        long start = now - (now % size);
        return Collections.singletonList(new TimeWindow(start, start + size));
    }

    public long getSize() {
        return size;
    }

    @Override
    public Trigger<T> getDefaultTrigger(TopologyContext context) {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "TumblingProcessingTimeWindows(" + size + ")";
    }

    /**
     * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
     * elements to time windows based on the element timestamp.
     *
     * @param size The size of the generated windows.
     * @return The time policy.
     */
    public static <T> TumblingIngestionTimeWindows<T> of(long size) {
        return new TumblingIngestionTimeWindows<>(size);
    }
}
