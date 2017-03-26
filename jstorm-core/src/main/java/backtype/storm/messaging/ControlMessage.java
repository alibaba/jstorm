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

public enum ControlMessage implements NettyMessage{
    EOB_MESSAGE((short) -201), OK_RESPONSE((short) -200);

    private short code;
    private long timeStamp;
    protected static int port;

    static public void setPort(int port) {
        ControlMessage.port = port;
    }

    // private constructor
    private ControlMessage(short code) {
        this.code = code;
    }

    /**
     * Return a control message per an encoded status code
     *
     * @param encoded
     * @return
     */
    public static ControlMessage mkMessage(short encoded) {
        for (ControlMessage cm : ControlMessage.values()) {
            if (encoded == cm.code)
                return cm;
        }
        return null;
    }

    
    @Override
    public int getEncodedLength() {
        // TODO Auto-generated method stub
        return 14; // short + long + int
    }

    /**
     * encode the current Control Message into a channel buffer
     *
     * @throws Exception
     */
    @Override
    public ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(getEncodedLength()));
        write(bout);
        bout.close();
        return bout.buffer();
    }

    public void write(ChannelBufferOutputStream bout) throws Exception {
        bout.writeShort(code);
        bout.writeLong(System.currentTimeMillis());
        bout.writeInt(port);
    }

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    
}
