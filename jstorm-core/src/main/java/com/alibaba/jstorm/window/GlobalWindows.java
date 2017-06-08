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
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that assigns all elements to the same global window.
 */
public class GlobalWindows<T> extends WindowAssigner<T> {
    private static final long serialVersionUID = 1L;

    private GlobalWindows() {
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        return Collections.singletonList((TimeWindow) GlobalWindow.get());
    }

    @Override
    public Trigger<T> getDefaultTrigger(TopologyContext topologyContext) {
        return new NeverTrigger<>();
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
    public static <T> GlobalWindows<T> create() {
        return new GlobalWindows<>();
    }

    /**
     * A trigger that never fires, as default Trigger for GlobalWindows.
     */
    private static class NeverTrigger<T> extends Trigger<T> {
        private static final long serialVersionUID = 1L;

        @Override
        public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }
    }
}
