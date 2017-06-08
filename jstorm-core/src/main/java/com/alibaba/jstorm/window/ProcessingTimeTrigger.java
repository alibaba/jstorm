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

public class ProcessingTimeTrigger<T> extends Trigger<T> {
    private static final long serialVersionUID = 1L;

    private ProcessingTimeTrigger() {
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) {
        ctx.registerProcessingTimeTimer(window.getEnd(), window);
        return TriggerResult.CONTINUE;
    }

    @Override
    public String toString() {
        return "ProcessingTimeTrigger()";
    }

    /**
     * Creates a new trigger that fires once system time passes the end of the window.
     */
    public static <T> ProcessingTimeTrigger<T> create() {
        return new ProcessingTimeTrigger<>();
    }

}
