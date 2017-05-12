/*
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

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;

/**
 * A {@code Trigger} determines when a pane of a window should be evaluated to emit the
 * results for that part of the window.
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 */
public abstract class Trigger<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Called for every element that gets added to a pane. The result of this will determine
     * whether the pane is evaluated to emit results.
     *
     * @param element   The element that arrived.
     * @param timestamp The timestamp of the element that arrived.
     * @param window    The window to which the element is being added.
     * @param ctx       A context object that can be used to register timer callbacks.
     */
    public abstract TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx);

    // ------------------------------------------------------------------------

    /**
     * A context object that is given to {@link Trigger} methods to allow them to register timer
     * callbacks and deal with state.
     */
    public interface TriggerContext {

        /**
         * Get max lag time in ms
         */
        long getMaxLagMs();

        /**
         * Returns the current watermark time.
         */
        long getCurrentWatermark();

        /**
         * Get pending windows
         */
        Collection<TimeWindow> getPendingWindows();

        /**
         * Register a system time callback.
         *
         * @param time   the timestamp at which to trigger the timer
         * @param window the window to which current input belongs
         */
        void registerProcessingTimeTimer(long time, TimeWindow window);

        /**
         * Register an event-time callback.
         *
         * @param time   the timestamp at which to trigger the timer
         * @param window the window to which current input belongs
         */
        void registerEventTimeTimer(long time, TimeWindow window);

        /**
         * Delete the processing time trigger for the given time.
         */
        ScheduledFuture<?> deleteProcessingTimeTimer(TimeWindow window);

        /**
         * Delete the event-time trigger for the given time.
         */
        ScheduledFuture<?> deleteEventTimeTimer(TimeWindow window);

    }

}
