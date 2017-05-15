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
package com.alibaba.jstorm.transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;

import com.alibaba.jstorm.cache.RocksDBCache;
import com.alibaba.jstorm.client.ConfigExtension;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RocksDbCacheOperator extends RocksDBCache implements ICacheOperator {
    public static Logger LOG = LoggerFactory.getLogger(RocksDbCacheOperator.class);

    private Map stormConf;

    private int maxFlushSize;

    private Kryo kryo;
    private Output output;
    private Input input;

    public RocksDbCacheOperator(TopologyContext context, String cacheDir) {
        this.stormConf = context.getStormConf();

        this.maxFlushSize = ConfigExtension.getTransactionCacheBatchFlushSize(stormConf);

        Options rocksDbOpt = new Options();
        rocksDbOpt.setCreateMissingColumnFamilies(true).setCreateIfMissing(true);
        long bufferSize =
                ConfigExtension.getTransactionCacheBlockSize(stormConf) != null ? ConfigExtension.getTransactionCacheBlockSize(stormConf) : (1 * SizeUnit.GB);
        rocksDbOpt.setWriteBufferSize(bufferSize);
        int maxBufferNum = ConfigExtension.getTransactionMaxCacheBlockNum(stormConf) != null ? ConfigExtension.getTransactionMaxCacheBlockNum(stormConf) : 3;
        rocksDbOpt.setMaxWriteBufferNumber(maxBufferNum);

        // Config for log of RocksDb
        rocksDbOpt.setMaxLogFileSize(1073741824); // 1G
        rocksDbOpt.setKeepLogFileNum(1);
        rocksDbOpt.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
        
        try {
            Map<Object, Object> conf = new HashMap<Object, Object>();
            conf.put(ROCKSDB_ROOT_DIR, cacheDir);
            conf.put(ROCKSDB_RESET, true);
            initDir(conf);
            initDb(null, rocksDbOpt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        kryo = new Kryo();
        output = new Output(200, 2000000000);
        input = new Input(1);

        LOG.info("Finished rocksDb cache init: maxFlushSize={}, bufferSize={}, maxBufferNum={}", maxFlushSize, bufferSize, maxBufferNum);
    }

    public int getMaxFlushSize() {
        return maxFlushSize;
    }

    @Override 
    protected byte[] serialize(Object obj) { 
        output.clear(); 
        kryo.writeObject(output, obj);
        return output.toBytes(); 
    }

    @Override 
    protected Object deserialize(byte[] data) { 
        input.setBuffer(data); 
        return kryo.readObject(input, ArrayList.class); 
    }

    public PendingBatch createPendingBatch(long batchId) {
        return new RocksDbPendingBatch(this, batchId);
    }
}