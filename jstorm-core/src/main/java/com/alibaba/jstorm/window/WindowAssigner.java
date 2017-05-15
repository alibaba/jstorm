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
import java.io.Serializable;
import java.util.Collection;

/**
 * @author wange
 * @since 16/12/8
 */
public abstract class WindowAssigner<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Returns a {@code Collection} of windows that should be assigned to the element.
     *
     * @param element   The element to which windows should be assigned.
     * @param timestamp The timestamp of the element.
     */
    public abstract Collection<TimeWindow> assignWindows(T element, long timestamp);

    /**
     * Returns the default trigger associated with this {@code WindowAssigner}.
     */
    public abstract Trigger<T> getDefaultTrigger(TopologyContext topologyContext);

    public static boolean isEventTime(WindowAssigner windowAssigner) {
        return windowAssigner instanceof SlidingEventTimeWindows ||
                windowAssigner instanceof TumblingEventTimeWindows ||
                windowAssigner instanceof EventTimeSessionWindows;
    }

    public static boolean isProcessingTime(WindowAssigner windowAssigner) {
        return windowAssigner instanceof SlidingProcessingTimeWindows ||
                windowAssigner instanceof TumblingProcessingTimeWindows ||
                windowAssigner instanceof ProcessingTimeSessionWindows;
    }

    public static boolean isSessionTime(WindowAssigner windowAssigner) {
        return windowAssigner instanceof ProcessingTimeSessionWindows ||
                windowAssigner instanceof EventTimeSessionWindows;
    }

    public static boolean isIngestionTime(WindowAssigner windowAssigner) {
        return windowAssigner instanceof TumblingIngestionTimeWindows ||
                windowAssigner instanceof SlidingIngestionTimeWindows;
    }

}
