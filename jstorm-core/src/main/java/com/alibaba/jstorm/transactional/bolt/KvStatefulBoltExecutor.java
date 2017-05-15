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

import static org.slf4j.LoggerFactory.getLogger;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.transactional.state.IKvState;
import com.alibaba.jstorm.transactional.state.KvTransactionStateOperator;
import com.alibaba.jstorm.utils.Thrift;

public abstract class KvStatefulBoltExecutor<K, V> extends KvTransactionStateOperator<K, V> implements ITransactionStatefulBoltExecutor {
    private static final long serialVersionUID = 4000012973628421732L;

    private static final Logger LOG = getLogger(KvStatefulBoltExecutor.class);

    private Map<String, Fields> fieldGrouping = Maps.newHashMap();
    
    public abstract void prepare(Map stormConf, TopologyContext context, OutputCollector collector, IKvState<K, V> state);

    public abstract void execute(Tuple input, IKvState<K, V> state);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        createState(context);
        prepare(stormConf, context, collector, keyRangeState);
        
        Map<GlobalStreamId, Grouping> sources = context.getSources(context.getThisComponentId());
        for (Map.Entry<GlobalStreamId, Grouping> entry : sources.entrySet()) {
            GlobalStreamId stream = entry.getKey();
            Grouping grouping = entry.getValue();
            Grouping._Fields groupingFields = Thrift.groupingType(grouping);
            if (Grouping._Fields.FIELDS.equals(groupingFields)) {
                Fields fields = new Fields(Thrift.fieldGrouping(grouping));
                fieldGrouping.put(stream.get_streamId(), fields);
            }
        }
        LOG.info("Source fieldgrouping streams: {}", fieldGrouping);
    }

    @Override
    public void execute(Tuple input) {
        IKvState<K, V> state = null;
        if (fieldGrouping.containsKey(input.getSourceStreamId())) {
            state = getSpecifiedKeyRangeState(input, fieldGrouping.get(input.getSourceStreamId()));
        }
        execute(input, state);
    }
    
    private IKvState<K, V> getSpecifiedKeyRangeState(Tuple input, Fields fields) {
        Object key = null;
        if (fields.size() == 1) {
            key = input.getValueByField(fields.get(0));
        } else {
            List<Object> fieldedValuesTobeHash = Lists.newArrayList();
            for (String field : fields) {
                fieldedValuesTobeHash.add(input.getValueByField(field));
            }
            key = fieldedValuesTobeHash;
        }
        return keyRangeState.getRangeStateByKey(key);
    }
}