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
package com.alibaba.jstorm.callback.impl;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set the topology status as Active
 * 
 */
public class ActiveTransitionCallback extends BaseCallback {

    private static Logger LOG = LoggerFactory.getLogger(ActiveTransitionCallback.class);

    @Override
    public <T> Object execute(T... args) {

        return new StormStatus(StatusType.active);
    }

}
