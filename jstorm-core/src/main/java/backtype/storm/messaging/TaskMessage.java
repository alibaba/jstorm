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
package backtype.storm.messaging;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
public class TaskMessage implements NettyMessage{
    public final static short NORMAL_MESSAGE = 0;
    public final static short CONTROL_MESSAGE = 1;
    public final static short BACK_PRESSURE_REQUEST = 2;
    private final short _type; //0 means taskmessage , 1 means task controlmessage
    private int _task;
    private byte[] _message;

    public TaskMessage(int task, byte[] message) {
        _type = NORMAL_MESSAGE;
        _task = task;
        _message = message;
    }
    public TaskMessage(short type, int task, byte[] message) {
        _type = type;
        _task = task;
        _message = message;
    }

    public short get_type() {
        return _type;
    }

    public int task() {
        return _task;
    }

    public byte[] message() {
        return _message;
    }
    

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        if (_message == null || _message.length == 0) {
            return true;
        }
        return false;
    }
    
    @Override
    public int getEncodedLength() {
        // TODO Auto-generated method stub
        if (_message == null) {
            return 0;
        }
        
        return _message.length;
    }

    /**
     * create a buffer containing the encoding of this batch
     */
    @Override
    public ChannelBuffer buffer() throws Exception {
        int payloadLen = 0;
        if (_message != null)
            payloadLen = _message.length;

        int totalLen = 8 + payloadLen;
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(totalLen));

        bout.writeShort(_type);

        if (_task > Short.MAX_VALUE)
            throw new RuntimeException("Task ID should not exceed " + Short.MAX_VALUE);

        bout.writeShort((short) _task);
        bout.writeInt(payloadLen);
        if (payloadLen > 0)
            bout.write(_message);
        
        bout.close();
        return bout.buffer();
    }
    
    
}
