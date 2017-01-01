package com.alibaba.jstorm.transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TransactionOutputFieldsDeclarer implements OutputFieldsDeclarer {
    private Map<String, StreamInfo> _fields = new HashMap<String, StreamInfo>();

    public void declare(Fields fields) {
        declare(false, fields);
    }

    public void declare(boolean direct, Fields fields) {
        declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
    }

    public void declareStream(String streamId, Fields fields) {
        declareStream(streamId, false, fields);
    }

    public void declareStream(String streamId, boolean direct, Fields fields) {
        if (_fields.containsKey(streamId)) {
            throw new IllegalArgumentException("Fields for " + streamId
                    + " already set");
        }
        List<String> fieldList = new ArrayList<String>();
        fieldList.add(TransactionCommon.BATCH_GROUP_ID_FIELD);
        fieldList.addAll(fields.toList());
        _fields.put(streamId, new StreamInfo(fieldList, direct));
    }

    public Map<String, StreamInfo> getFieldsDeclaration() {
        return _fields;
    }
}