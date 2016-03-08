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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.Utils;

import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class KryoTupleDeserializer implements ITupleDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(KryoTupleDeserializer.class);

    public static final boolean USE_RAW_PACKET = true;

    GeneralTopologyContext _context;
    KryoValuesDeserializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Input _kryoInput;

    public KryoTupleDeserializer(final Map conf, final GeneralTopologyContext context) {
        _kryo = new KryoValuesDeserializer(conf);
        _context = context;
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        _kryoInput = new Input(1);
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

        try {
            targetTaskId = input.readInt();
            timeStamp = input.readLong();
            taskId = input.readInt(true);
            streamId = input.readInt(true);
            componentName = _context.getComponentId(taskId);
            streamName = _ids.getStreamName(componentName, streamId);
            id = MessageId.deserialize(input);
            List<Object> values = _kryo.deserializeFrom(input);
            TupleImplExt tuple = new TupleImplExt(_context, values, taskId, streamName, id);
            tuple.setTargetTaskId(targetTaskId);
            tuple.setCreationTimeStamp(timeStamp);
            return tuple;
        } catch (Throwable e) {
            StringBuilder sb = new StringBuilder();

            sb.append("Deserialize error:");
            sb.append("targetTaskId:").append(targetTaskId);
            sb.append(",creationTimeStamp:").append(timeStamp);
            sb.append(",taskId:").append(taskId);
            sb.append(",streamId:").append(streamId);
            sb.append(",componentName:").append(componentName);
            sb.append(",streamName:").append(streamName);
            sb.append(",MessageId").append(id);

            LOG.info(sb.toString(), e);
            throw new RuntimeException(e);
        }
    }

    public BatchTuple deserializeBatch(byte[] ser) {
        BatchTuple ret = new BatchTuple();
        
        int offset = 0;
        while(offset < ser.length) {
            _kryoInput.setBuffer(ser, offset, offset + 4);
            int tupleSize = _kryoInput.readInt();
            offset += 4;

            ret.addToBatch(deserialize(ser, offset, offset + tupleSize));
            offset += tupleSize;
        }

        return ret;
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
