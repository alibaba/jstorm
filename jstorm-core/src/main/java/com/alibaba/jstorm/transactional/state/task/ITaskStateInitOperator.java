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
package com.alibaba.jstorm.transactional.state.task;

import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.transactional.state.TransactionState;

public interface ITaskStateInitOperator {
    /**
     * 
     * @param conf Topoloyg config
     * @param currTaskId Id of the task that is going to be init
     * @param currComponentTasks All current tasks which belong to this component 
     * @param prevComponentTaskStates States of the previous tasks which belong to this component
     * @return task state
     */
    public Object getTaskInitState(Map conf, int currTaskId, Set<Integer> currComponentTasks, Map<Integer, TransactionState> prevComponentTaskStates);
}