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
package com.alibaba.jstorm.daemon.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.esotericsoftware.kryo.KryoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.utils.DisruptorRunable;

/**
 * control message dispatcher
 *
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class VirtualPortCtrlDispatch extends DisruptorRunable {
    private static final Logger LOG = LoggerFactory.getLogger(VirtualPortCtrlDispatch.class);

    protected ConcurrentHashMap<Integer, DisruptorQueue> controlQueues;
    protected IConnection recvConnection;
    protected AtomicReference<KryoTupleDeserializer> atomKryoDeserializer;

    public VirtualPortCtrlDispatch(WorkerData workerData, IConnection recvConnection,
                                   DisruptorQueue recvQueue, String idStr) {
        super(recvQueue, idStr);

        this.recvConnection = recvConnection;
        this.controlQueues = workerData.getControlQueues();
        this.atomKryoDeserializer = workerData.getAtomKryoDeserializer();
    }

    public void shutdownRecv() {
        // don't need send shutdown command to every task
        // due to every task has been shutdown by workerData.active
        // at the same time queue has been fulll
        // byte shutdownCmd[] = { TaskStatus.SHUTDOWN };
        // for (DisruptorQueue queue : deserializeQueues.values()) {
        //
        // queue.publish(shutdownCmd);
        // }

        try {
            recvConnection.close();
        } catch (Exception ignored) {
        }
        recvConnection = null;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        LOG.info("Begin to shutdown VirtualPortCtrlDispatch");
        shutdownRecv();
        LOG.info("Successfully shutdown VirtualPortCtrlDispatch");
    }

    @SuppressWarnings("unused")
    protected Object deserialize(byte[] serMsg, int taskId) {
        try {
            if (serMsg == null) {
                return null;
            }

            if (serMsg.length == 0) {
                return null;
            } else if (serMsg.length == 1) {
                //ignore
                return null;
            }
            Tuple tuple = null;
            // serMsg.length > 1
            KryoTupleDeserializer kryo = atomKryoDeserializer.get();
            if (kryo != null) {
                tuple = kryo.deserialize(serMsg);
            }
            return tuple;
        } catch (Throwable e) {
            if (Utils.exceptionCauseIsInstanceOf(KryoException.class, e))
                throw new RuntimeException(e);
            LOG.error(idStr + " recv thread error " + JStormUtils.toPrintableString(serMsg) + "\n", e);
        }
        return null;
    }

    @Override
    public void handleEvent(Object event, boolean endOfBatch) throws Exception {
        TaskMessage message = (TaskMessage) event;
        int task = message.task();

        Object tuple = null;
        try {
            //there might be errors when calling update_topology
            tuple = deserialize(message.message(), task);
        } catch (Throwable e) {
            if (Utils.exceptionCauseIsInstanceOf(KryoException.class, e))
                throw new RuntimeException(e);
            LOG.warn("serialize msg error", e);
        }

        DisruptorQueue queue = controlQueues.get(task);
        if (queue == null) {
            LOG.warn("Received invalid control message for task-{}, Dropping...{} ", task, tuple);
            return;
        }
        if (tuple != null) {
            queue.publish(tuple);
        }
    }

}
