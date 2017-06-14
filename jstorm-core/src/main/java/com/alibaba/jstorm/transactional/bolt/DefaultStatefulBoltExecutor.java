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
package com.alibaba.jstorm.transactional.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.transactional.state.IKvState;
import com.alibaba.jstorm.transactional.state.TransactionStateOperator;

public abstract class DefaultStatefulBoltExecutor<K, V> extends TransactionStateOperator<K, V> implements ITransactionStatefulBoltExecutor {
    private static final long serialVersionUID = 1638411046827664146L;

    private IKvState<K, V> state;

    public abstract void prepare(Map stormConf, TopologyContext context, OutputCollector collector, IKvState<K, V> state);

    public abstract void execute(Tuple input, IKvState<K, V> state);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.state = createState(context);
        prepare(stormConf, context, collector, state);
    }

    @Override
    public void execute(Tuple input) {
        execute(input, state);
    }
    
}