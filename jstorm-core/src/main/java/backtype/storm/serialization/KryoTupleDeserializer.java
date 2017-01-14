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
package backtype.storm.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.topology.IProtoBatchBolt;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;

import com.alibaba.jstorm.utils.Pair;
import com.esotericsoftware.kryo.io.Input;

public class KryoTupleDeserializer implements ITupleDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(KryoTupleDeserializer.class);

    public static final boolean USE_RAW_PACKET = true;

    GeneralTopologyContext _context;
    KryoValuesDeserializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Input _kryoInput;
    int _ackerNum;

    public KryoTupleDeserializer(final Map conf, final GeneralTopologyContext context, final StormTopology topology) {
        _kryo = new KryoValuesDeserializer(conf);
        _context = context;
        _ids = new SerializationFactory.IdDictionary(topology);
        _kryoInput = new Input(1);
        _ackerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 0);
    }

    public Tuple deserialize(byte[] ser) {
        _kryoInput.setBuffer(ser);
        return deserialize(_kryoInput);
    }

    public Tuple deserialize(byte[] ser, int offset, int count) {
        _kryoInput.setBuffer(ser, offset, count);
        return deserialize(_kryoInput);
    }

    public Tuple deserialize(Input input) {
        int targetTaskId = 0;
        long timeStamp = 0l;
        int taskId = 0;
        int streamId = 0;
        String componentName = null;
        String streamName = null;
        MessageId id = null;
        boolean isBatchTuple = false;

        try {
            targetTaskId = input.readInt();
            timeStamp = input.readLong();
            isBatchTuple = input.readBoolean();
            taskId = input.readInt(true);
            streamId = input.readInt(true);
            componentName = _context.getComponentId(taskId);
            streamName = _ids.getStreamName(componentName, streamId);

            List<Object> values = null;
            if (isBatchTuple) {
            	values = new ArrayList<Object>();
            	int len = input.readInt(true);
                if (_ackerNum > 0){
                    for (int i = 0; i < len; i++) {
                        values.add(new Pair<MessageId, List<Object>>(MessageId.deserialize(input), _kryo.deserializeFrom(input)));
                    }
                }else {
                    for (int i = 0; i < len; i++) {
                        values.add(new Pair<MessageId, List<Object>>(null, _kryo.deserializeFrom(input)));
                    }
                }
            } else {
                id = MessageId.deserialize(input);
                values = _kryo.deserializeFrom(input);
            }
            TupleImplExt tuple = new TupleImplExt(_context, values, taskId, streamName, id);
            tuple.setBatchTuple(isBatchTuple);
            tuple.setTargetTaskId(targetTaskId);
            tuple.setCreationTimeStamp(timeStamp);
            return tuple;
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();

            sb.append("Deserialize error:");
            sb.append("targetTaskId:").append(targetTaskId);
            sb.append(",creationTimeStamp:").append(timeStamp);
            sb.append(",isBatchTuple:").append(isBatchTuple);
            sb.append(",taskId:").append(taskId);
            sb.append(",streamId:").append(streamId);
            sb.append(",componentName:").append(componentName);
            sb.append(",streamName:").append(streamName);
            sb.append(",MessageId").append(id);
            LOG.error("Kryo error!!! {} {}", sb.toString(), e);
            throw new RuntimeException(e);
        }
    }
    /**
     * just get target taskId
     * 
     * @param ser
     * @return
     */
    public static int deserializeTaskId(byte[] ser) {
        Input _kryoInput = new Input(1);

        _kryoInput.setBuffer(ser);

        int targetTaskId = _kryoInput.readInt();

        return targetTaskId;
    }
}
