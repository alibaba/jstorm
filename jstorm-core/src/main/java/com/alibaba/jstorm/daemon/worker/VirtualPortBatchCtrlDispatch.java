package com.alibaba.jstorm.daemon.worker;

import backtype.storm.messaging.ControlMessage;
import backtype.storm.messaging.IConnection;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.utils.DisruptorQueue;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class VirtualPortBatchCtrlDispatch extends VirtualPortCtrlDispatch {
    private final static Logger LOG = LoggerFactory.getLogger(VirtualPortBatchCtrlDispatch.class);

    public VirtualPortBatchCtrlDispatch(WorkerData workerData, IConnection recvConnection, DisruptorQueue recvQueue, String idStr) {
        super(workerData, recvConnection, recvQueue, idStr);
    }

    @Override
    protected Object deserialize(byte[] ser_msg, int taskId) {
        try {
            if (ser_msg == null) {
                return null;
            }

            if (ser_msg.length == 0) {
                return null;
            } else if (ser_msg.length == 1) {
                byte newStatus = ser_msg[0];
                LOG.info("Change task status as " + newStatus);
                taskStatus.setStatus(newStatus);

                return null;
            }
            // ser_msg.length > 1
            BatchTuple tuple = null;
            KryoTupleDeserializer kryo = atomKryoDeserializer.get();
            if (kryo != null)
                tuple = kryo.deserializeBatch(ser_msg);

            return tuple;
        } catch (Throwable e) {
            if (!taskStatus.isShutdown()) {
                LOG.error(idStr + " recv thread error " + JStormUtils.toPrintableString(ser_msg) + "\n", e);
            }
        }

        return null;
    }

}
