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

import java.nio.ByteBuffer;

public class TaskMessage {
    private final short _type; //0 means taskmessage , 1 means task controlmessage
    private int _task;
    private byte[] _message;

    public TaskMessage(int task, byte[] message) {
        _type = 0;
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

    public static boolean isEmpty(TaskMessage message) {
        if (message == null) {
            return true;
        } else if (message.message() == null) {
            return true;
        } else if (message.message().length == 0) {
            return true;
        }

        return false;
    }

/*    @Deprecated
    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(_message.length + 4);
        bb.putShort((short) _type);
        bb.putShort((short) _task);
        bb.put(_message);
        return bb;
    }

    @Deprecated
    public void deserialize(ByteBuffer packet) {
        if (packet == null)
            return;
        _type = packet.getShort();
        _task = packet.getShort();
        _message = new byte[packet.limit() - 4];
        packet.get(_message);
    }*/

}
