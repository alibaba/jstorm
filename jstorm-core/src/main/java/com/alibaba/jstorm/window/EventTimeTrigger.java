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

public class EventTimeTrigger<T> extends Trigger<T> {
    private static final long serialVersionUID = 1L;

    private EventTimeTrigger() {
    }

    /**
     * If a watermark arrives, we need to check all pending windows.
     * If any of the pending window suffices, we should fire immediately by registering a timer without delay.
     * Otherwise we register a timer whose time is the window end plus max lag time
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        ctx.registerEventTimeTimer(window.getEnd() + ctx.getMaxLagMs(), window);
        return TriggerResult.CONTINUE;
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>
     * Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static <T> EventTimeTrigger<T> create() {
        return new EventTimeTrigger<>();
    }

}
