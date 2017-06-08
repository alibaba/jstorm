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
package com.alibaba.jstorm.transactional.state;

import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;

public class DefaultTopologyStateOperator implements IRichTopologyStateOperator {
    private Map conf;

    @Override
    public Object initState(String topologyName) {
        return null;
    }

    @Override
    public boolean commitState(String topologyName, Object state) {
        return true;
    }

    @Override
    public void init(TopologyContext context) {
        this.conf = context.getStormConf();
    }

    @Override
    public Object getInitSpoutUserState(int currTaskId, Set<Integer> currSpoutTasks, Map<Integer, TransactionState> prevSpoutStates) {
        Object ret = null;
        if (prevSpoutStates != null) {
            TransactionState state = prevSpoutStates.get(currTaskId);
            ret = state != null ? state.getUserCheckpoint() : null;
        }
        return ret;
    }

    @Override
    public Object getInitBoltUserState(int currTaskId, Set<Integer> currBoltTasks, Map<Integer, TransactionState> prevBoltStates) {
        Object ret = null;
        if (prevBoltStates != null) {
            TransactionState state = prevBoltStates.get(currTaskId);
            ret = state != null ? state.getUserCheckpoint() : null;
        }
        return ret;
    }

    @Override
    public Object getInitSpoutSysState(int currTaskId, Set<Integer> currSpoutTasks, Map<Integer, TransactionState> prevSpoutStates) {
        Object ret = null;
        if (prevSpoutStates != null) {
            TransactionState state = prevSpoutStates.get(currTaskId);
            ret = state != null ? state.getsysCheckpoint() : null;
        }
        return ret;
    }

    @Override
    public Object getInitBoltSysState(int currTaskId, Set<Integer> currBoltTasks, Map<Integer, TransactionState> prevBoltStates) {
        Object ret = null;
        if (prevBoltStates != null) {
            TransactionState state = prevBoltStates.get(currTaskId);
            ret = state != null ? state.getsysCheckpoint() : null;
        }
        return ret;
    }
}