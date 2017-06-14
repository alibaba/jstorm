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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 * A bolt abstraction for supporting time and count based sliding & tumbling windows.
 */
interface IWindowedBolt<T extends Tuple> extends IComponent {

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void cleanup();

    /**
     * Init window state. This will be called before calling execute method.
     *
     * @return user-defined window state
     */
    Object initWindowState(TimeWindow window);

    /**
     * Execute a tuple within a window. If a tuple belongs to multiple windows,
     * all windows will be iterated and this method will be called on each window.
     *
     * @param tuple  the tuple arrived
     * @param state  user-defined window state, note that currently only reference objects are supported, primitive
     *               type states are not supported. you are supposed to operate directly on state and the framework
     *               will update the state automatically.
     * @param window the window to which the tuple belongs
     */
    void execute(T tuple, Object state, TimeWindow window);

    /**
     * Purge the window. Called after the window is computed. Users may emit the whole state or a part of it.
     *
     * @param state  user-defined window state,
     * @param window the window to be purged.
     */
    void purgeWindow(Object state, TimeWindow window);

}
