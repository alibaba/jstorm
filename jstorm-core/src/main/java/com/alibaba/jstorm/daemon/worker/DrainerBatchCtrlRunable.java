package com.alibaba.jstorm.daemon.worker;

import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class DrainerBatchCtrlRunable extends DrainerCtrlRunable {

    private final static Logger LOG = LoggerFactory.getLogger(DrainerBatchCtrlRunable.class);

    public DrainerBatchCtrlRunable(WorkerData workerData, String idStr) {
        super(workerData, idStr);
    }

    protected byte[] serialize(ITupleExt tuple) {
        byte[] bytes = null;
        KryoTupleSerializer kryo = atomKryoSerializer.get();
        if (kryo != null) {
            bytes = kryo.serializeBatch((BatchTuple) tuple);
        } else {
            LOG.warn(" KryoTupleSerializer is null, so drop batchTuple....");
        }

        return bytes;
    }
}
