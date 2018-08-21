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

import java.util.List;

import org.jboss.netty.channel.Channel;

import backtype.storm.utils.DisruptorQueue;

public interface IConnection {

    /**
     * (flags != 1) synchronously (flags==1) asynchronously
     */
    Object recv(Integer taskId, int flags);

    /**
     * In the new design, receive flow is through registerQueue, then push message into queue
     */
    void registerQueue(Integer taskId, DisruptorQueue recvQueu);

    void enqueue(TaskMessage message, Channel channel);

    void send(List<TaskMessage> messages);

    void send(TaskMessage message);

    void sendDirect(TaskMessage message);

    boolean available(int taskId);

    /**
     * close this connection
     */
    void close();

    boolean isClosed();
}
