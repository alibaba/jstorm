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
 * A {@link WindowAssigner} that windows elements into sessions based on the current processing
 * time.
 */
public class ProcessingTimeSessionWindows<T> extends WindowAssigner<T> {
    private static final long serialVersionUID = 1L;

    protected long sessionTimeout;

    protected ProcessingTimeSessionWindows(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
        return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
    }

    @Override
    public Trigger<T> getDefaultTrigger(TopologyContext context) {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "ProcessingTimeSessionWindows(" + sessionTimeout + ")";
    }

    /**
     * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns
     * elements to sessions based on the element timestamp.
     *
     * @param timeout The session timeout, i.e. the time gap between sessions
     * @return The policy.
     */
    public static <T> ProcessingTimeSessionWindows<T> withGap(long timeout) {
        return new ProcessingTimeSessionWindows<>(timeout);
    }
}
