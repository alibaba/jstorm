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

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
    KryoValuesSerializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Output _kryoOut;
    int _ackerNum;

    public KryoTupleSerializer(final Map conf, final StormTopology topology) {
        _kryo = new KryoValuesSerializer(conf);
        _kryoOut = new Output(2000, 2000000000);
        _ackerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 0);
        _ids = new SerializationFactory.IdDictionary(topology);
    }

    public byte[] serialize(Tuple tuple) {
        _kryoOut.clear();
        serializeTuple(_kryoOut, tuple);
        return _kryoOut.toBytes();
    }
    /**
     * @@@ in the furture, it will skill serialize 'targetTask' through check some flag
     * @see backtype.storm.serialization.ITupleSerializer#serialize(int, backtype.storm.tuple.Tuple)
     */
    private void serializeTuple(Output output, Tuple tuple) {
        try {
        	boolean isBatchTuple = false;
            if (tuple instanceof TupleExt) {
                output.writeInt(((TupleExt) tuple).getTargetTaskId());
                output.writeLong(((TupleExt) tuple).getCreationTimeStamp());
                output.writeBoolean(((TupleExt) tuple).isBatchTuple());
                isBatchTuple = ((TupleExt) tuple).isBatchTuple();
            }

            output.writeInt(tuple.getSourceTask(), true);
            output.writeInt(_ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()), true);

            if (isBatchTuple) {
            	List<Object> values = tuple.getValues();
                int len = values.size();
                output.writeInt(len, true);
                if (_ackerNum > 0){
                    for (Object value : values) {
                        Pair<MessageId, List<Object>> pairValue = (Pair<MessageId, List<Object>>) value;
                        if (pairValue.getFirst() != null) {
                            pairValue.getFirst().serialize(output);
                        } else {
                            output.writeInt(0, true);
                        }
                        _kryo.serializeInto(pairValue.getSecond(), output);
                    }
                } else {
                    for (Object value : values) {
                        Pair<MessageId, List<Object>> pairValue = (Pair<MessageId, List<Object>>) value;
                        _kryo.serializeInto(pairValue.getSecond(), output);
                    }
                }
            } else {
                MessageId msgId = tuple.getMessageId();
                if (msgId != null) {
                    msgId.serialize(output);
                } else {
                    output.writeInt(0, true);
                }
                _kryo.serializeInto(tuple.getValues(), output);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(int targetTask) {
        ByteBuffer buff = ByteBuffer.allocate((Integer.SIZE / 8));
        buff.putInt(targetTask);
        byte[] rtn = buff.array();
        return rtn;
    }

    // public long crc32(Tuple tuple) {
    // try {
    // CRC32OutputStream hasher = new CRC32OutputStream();
    // _kryo.serializeInto(tuple.getValues(), hasher);
    // return hasher.getValue();
    // } catch (IOException e) {
    // throw new RuntimeException(e);
    // }
    // }
}
