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
package com.alibaba.jstorm.message.netty;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.ControlMessage;
import backtype.storm.messaging.NettyMessage;
import backtype.storm.messaging.TaskMessage;

class MessageBatch implements NettyMessage{
    private static final Logger LOG = LoggerFactory.getLogger(MessageBatch.class);
    private int buffer_size;
    private ArrayList<Object> msgs;
    private int encodedLength;

    MessageBatch(int buffer_size) {
        this.buffer_size = buffer_size;
        msgs = new ArrayList<Object>();
        encodedLength = ControlMessage.EOB_MESSAGE.getEncodedLength();
    }

    void add(NettyMessage obj) {
        if (obj == null)
            throw new RuntimeException("null object forbidded in message batch");

        if (obj instanceof TaskMessage) {
            TaskMessage msg = (TaskMessage) obj;
            msgs.add(msg);
            encodedLength += msgEncodeLength(msg);
            return;
        }

        if (obj instanceof MessageBatch){
            MessageBatch batch = (MessageBatch) obj;
            add(batch);
            return;
        }

        if (obj instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage) obj;
            msgs.add(msg);
            encodedLength += msg.getEncodedLength();
            return;
        }

        throw new RuntimeException("Unsuppoted object type " + obj.getClass().getName());
    }

    void add(List objs) {
        if (objs == null || objs.size() == 0)
            throw new RuntimeException("null object forbidded in message batch");

        if (objs.get(0) instanceof TaskMessage) {
            for (Object obj : objs) {
                TaskMessage msg = (TaskMessage) obj;
                msgs.add(msg);
                encodedLength += msgEncodeLength(msg);
            }
            return;
        }

        if (objs.get(0) instanceof ControlMessage) {
            for (Object obj : objs) {
                ControlMessage msg = (ControlMessage) obj;
                msgs.add(msg);
                encodedLength += msg.getEncodedLength();
            }
            return;
        }

        throw new RuntimeException("Unsuppoted object type " + objs.get(0).getClass().getName());
    }

    public void add(MessageBatch batch) {
    	if (batch != null && batch.size() >= 0) {
            msgs.addAll(batch.getMessages());
            encodedLength += batch.getEncodedLength();
    	}
    }

    void remove(Object obj) {
        if (obj == null)
            return;

        if (obj instanceof TaskMessage) {
            TaskMessage msg = (TaskMessage) obj;
            msgs.remove(msg);
            encodedLength -= msgEncodeLength(msg);
            return;
        }

        if (obj instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage) obj;
            msgs.remove(msg);
            encodedLength -= msg.getEncodedLength();
            return;
        }
    }

    Object get(int index) {
        return msgs.get(index);
    }

    /**
     * try to add a TaskMessage to a batch
     *
     * @param taskMsg
     * @return false if the msg could not be added due to buffer size limit; true otherwise
     */
    boolean tryAdd(TaskMessage taskMsg) {
        if ((encodedLength + msgEncodeLength(taskMsg)) > buffer_size)
            return false;
        add(taskMsg);
        return true;
    }

    private int msgEncodeLength(TaskMessage taskMsg) {
        if (taskMsg == null)
            return 0;

        int size = 8; // INT + SHORT + SHORT
        if (taskMsg.message() != null)
            size += taskMsg.message().length;
        return size;
    }

    /**
     * Has this batch used up allowed buffer size
     *
     * @return
     */
    boolean isFull() {
        return encodedLength >= buffer_size;
    }

    /**
     * true if this batch doesn't have any messages
     *
     * @return
     */
    @Override
    public boolean isEmpty() {
        return msgs.isEmpty();
    }

    /**
     * # of msgs in this batch
     *
     * @return
     */
    int size() {
        return msgs.size();
    }

    public int getEncodedLength() {
        return encodedLength;
    }

    public List<Object> getMessages() {
    	return msgs;
    }

    /**
     * create a buffer containing the encoding of this batch
     */
    @Override
    public ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(encodedLength));

        for (Object msg : msgs)
            if (msg instanceof TaskMessage)
                writeTaskMessage(bout, (TaskMessage) msg);
            else {
                // LOG.debug("Write one non-TaskMessage {}", msg );
                ((ControlMessage) msg).write(bout);
            }

        // add a END_OF_BATCH indicator
        ControlMessage.EOB_MESSAGE.write(bout);
        // LOG.debug("ControlMessage.EOB_MESSAGE " );

        bout.close();

        return bout.buffer();
    }

    /**
     * write a TaskMessage into a stream
     *
     * Each TaskMessage is encoded as: task ... short(2) len ... int(4) payload ... byte[] *
     */
    private void writeTaskMessage(ChannelBufferOutputStream bout, TaskMessage message) throws Exception {
        int payload_len = 0;
        if (message.message() != null)
            payload_len = message.message().length;

        short type = message.get_type();
        bout.writeShort(type);

        int task_id = message.task();
        if (task_id > Short.MAX_VALUE)
            throw new RuntimeException("Task ID should not exceed " + Short.MAX_VALUE);

        bout.writeShort((short) task_id);
        bout.writeInt(payload_len);
        if (payload_len > 0)
            bout.write(message.message());

        // @@@ TESTING CODE
        // LOG.info("Write one message taskid:{}, len:{}, data:{}", task_id
        // , payload_len, JStormUtils.toPrintableString(message.message()) );
    }

    @Override
    public String toString() {
    	return "bufferSize=" + buffer_size + ", messageSize=" + msgs.size();
    }
}