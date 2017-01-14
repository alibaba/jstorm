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
package com.alibaba.jstorm.event;

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * put event to eventManager queue
 */
public class EventManagerPusher extends RunnableCallback {

    private EventManager eventManager;

    private RunnableCallback event;

    private int frequency;

    public EventManagerPusher(EventManager eventManager, RunnableCallback event, int frequency) {
        this.eventManager = eventManager;
        this.event = event;
        this.frequency = frequency;
    }

    @Override
    public Object getResult() {
        return frequency;
    }

    @Override
    public void run() {
        eventManager.add(event);
    }

}
