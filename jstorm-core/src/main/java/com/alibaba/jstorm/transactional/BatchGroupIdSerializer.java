package com.alibaba.jstorm.transactional;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BatchGroupIdSerializer extends Serializer<BatchGroupId> {

    @Override
    public void write(Kryo kryo, Output output, BatchGroupId object) {
        output.writeInt(object.groupId, true);
        output.writeLong(object.batchId, true);
    }

    @Override
    public BatchGroupId read(Kryo kryo, Input input, Class<BatchGroupId> type) {
        int groupId = input.readInt(true);
        long batchId = input.readLong(true);
        return new BatchGroupId(groupId, batchId);
    }
    
}