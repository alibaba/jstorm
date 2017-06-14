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
package com.alibaba.jstorm.task.execute;

import backtype.storm.task.ICollectorCallback;

import java.util.List;

/**
 * @author xiaojian.fxj
 */
public abstract class MsgInfo {
    public String streamId;
    public Integer outTaskId;
    public List<Object> values;
    public ICollectorCallback callback;

    public MsgInfo(String streamId, List<Object> values, Integer outTaskId) {
        this.streamId = streamId;
        this.values = values;
        this.outTaskId = outTaskId;
        this.callback = null;
    }

    public MsgInfo(String streamId, List<Object> values, Integer outTaskId, ICollectorCallback callback) {
        this.streamId = streamId;
        this.values = values;
        this.outTaskId = outTaskId;
        this.callback = callback;
    }
}