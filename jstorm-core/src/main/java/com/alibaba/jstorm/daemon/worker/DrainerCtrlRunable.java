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
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.KryoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.scheduler.WorkerSlot;
import com.alibaba.jstorm.utils.DisruptorRunable;


/**
 * send control message
 * 
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class DrainerCtrlRunable extends DisruptorRunable {
    private final static Logger LOG = LoggerFactory.getLogger(DrainerCtrlRunable.class);

    private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;
    protected AtomicReference<KryoTupleSerializer> atomKryoSerializer;

    public DrainerCtrlRunable(WorkerData workerData, String idStr) {
        super(workerData.getTransferCtrlQueue(), idStr);
        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();
        this.atomKryoSerializer = workerData.getAtomKryoSerializer();
    }

    protected IConnection getConnection(int taskId) {
        IConnection conn = null;
        WorkerSlot nodePort = taskNodeport.get(taskId);
        if (nodePort == null) {
            String errormsg = "IConnection to " + taskId + " can't be found";
            LOG.warn("Internal transfer warn, throw tuple,", new Exception(errormsg));
        } else {
            conn = nodeportSocket.get(nodePort);
            if (conn == null) {
                String errormsg = "NodePort to" + nodePort + " can't be found";
                LOG.warn("Internal transfer warn, throw tuple,", new Exception(errormsg));
            }
        }
        return conn;
    }

    protected byte[] serialize(ITupleExt tuple) {

        byte[] bytes = null;
        KryoTupleSerializer kryo = atomKryoSerializer.get();
        if (kryo != null) {
            bytes = kryo.serialize((TupleExt) tuple);
        } else {
            LOG.warn("KryoTupleSerializer is null, so drop tuple...");
        }
        return bytes;
    }

    @Override
    public void handleEvent(Object event, boolean endOfBatch) throws Exception {

        if (event == null) {
            return;
        }
        ITupleExt tuple = (ITupleExt) event;
        int targetTask = tuple.getTargetTaskId();

        IConnection conn = getConnection(targetTask);
        if (conn != null) {
            byte[] tupleMessage = null;
            try {
                //it maybe happened errors when update_topology
                tupleMessage = serialize(tuple);
            } catch (Throwable e) {
                if (Utils.exceptionCauseIsInstanceOf(KryoException.class, e)) {
                    throw new RuntimeException(e);
                } else {
                    LOG.warn("serialize happened errors!!!", e);
                }
            }
            TaskMessage message = new TaskMessage((short) TaskMessage.CONTROL_MESSAGE, targetTask, tupleMessage);
            conn.send(message);
        }
    }

}
