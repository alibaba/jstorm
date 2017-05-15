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
package com.alibaba.jstorm.hbase;

import com.alibaba.jstorm.metric.KVSerializable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader.*;
import static com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader.NIMBUS_CONF_PREFIX;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.1.1
 */
public abstract class AbstractHBaseClient {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String HBASE_QUORUM_KEY = "hbase.zookeeper.quorum";
    protected static final String HBASE_QUORUM_CONF_KEY = NIMBUS_CONF_PREFIX + HBASE_QUORUM_KEY;
    protected static final String HBASE_PORT_KEY = "hbase.zookeeper.property.clientPort";
    protected static final String HBASE_PORT_CONF_KEY = NIMBUS_CONF_PREFIX + HBASE_PORT_KEY;
    protected static final String HBASE_ZK_PARENT_KEY = "zookeeper.znode.parent";
    protected static final String HBASE_ZK_PARENT_CONF_KEY = NIMBUS_CONF_PREFIX + HBASE_ZK_PARENT_KEY;

    protected static final String TABLE_TASK_TRACK = "koala_task_track";
    protected static final String TABLE_TOPOLOGY_HISTORY = "koala_topology_history";
    protected static final String TABLE_METRIC_META = "koala_metric_meta";
    protected static final String TABLE_METRIC_DATA = "koala_metric_data";

    protected static final byte[] CF = Bytes.toBytes("v");
    protected static final byte[] V_DATA = Bytes.toBytes("d");

    protected static final long BUFFER_SIZE = 256 * 1024;
    protected static final int TABLE_POOL_SIZE = 4;
    protected static final int CACHE_SIZE = 100;
    protected static final int MAX_SCAN_ROWS = 500;
    protected static final int MAX_SCAN_META_ROWS = 100000;

    protected HTablePool hTablePool;

    public HTableInterface getHTableInterface(String tableName) {
        try {
            HTableInterface table = hTablePool.getTable(tableName);
            // disable auto flush, use buffer size: 256KB
            table.setAutoFlush(false);
            table.setWriteBufferSize(BUFFER_SIZE);

            return table;
        } catch (Exception ex) {
            logger.error("getHTableInterface error:", ex);
        }
        return null;
    }

    public void closeTable(HTableInterface table) {
        if (table != null) {
            try {
                table.close();
            } catch (Exception ignored) {
            }
        }
    }

    public Configuration makeConf(Map stormConf) {
        Configuration hbaseConf = HBaseConfiguration.create();
        String hbaseQuorum = (String) stormConf.get(HBASE_QUORUM_CONF_KEY);
        hbaseConf.set(HBASE_QUORUM_KEY, hbaseQuorum);

        String hbasePort = stormConf.get(HBASE_PORT_CONF_KEY) + "";
        hbaseConf.set(HBASE_PORT_KEY, hbasePort);

        String hbaseParent = (String) stormConf.get(HBASE_ZK_PARENT_CONF_KEY);
        hbaseConf.set(HBASE_ZK_PARENT_KEY, hbaseParent);

        return hbaseConf;
    }

    public void initFromStormConf(Map stormConf) {
        logger.info("init hbase client.");
        Configuration conf = makeConf(stormConf);
        hTablePool = new HTablePool(conf, TABLE_POOL_SIZE);
        logger.info("finished init hbase client.");
    }

    @SuppressWarnings("unchecked")
    protected <T extends KVSerializable> List<T> scanRows(String tableName, Class<T> clazz, byte[] start, byte[] end, int num) {
        HTableInterface table = getHTableInterface(tableName);
        Scan scan = new Scan(start, end);
        //scan.setBatch(10);
        scan.setCaching(CACHE_SIZE);
        int n = 0;
        List<T> items = new ArrayList<>(num);

        ResultScanner scanner = null;
        Constructor<T> ctor = null;
        for (Constructor<?> constructor : clazz.getConstructors()) {
            if (constructor.getParameterTypes().length == 0) {
                ctor = (Constructor<T>) constructor;
                break;
            }
        }

        if (ctor == null) {
            logger.error("Failed to find ctor which takes 0 arguments for class {}!", clazz.getSimpleName());
            return items;
        }

        try {
            scanner = getScanner(table, scan);
            for (Result result : scanner) {
                T item = ctor.newInstance();
                byte[] key = result.getRow();
                byte[] value = result.getValue(CF, V_DATA);

                item.fromKV(key, value);
                items.add(item);
                if (++n >= num) {
                    break;
                }
            }
        } catch (Exception ex) {
            logger.error("Scan error, table:{}, class:{}:", tableName, clazz.getSimpleName(), ex);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            closeTable(table);
        }
        return items;
    }

    protected KVSerializable getRow(String tableName, Class clazz, byte[] key) {
        HTableInterface table = getHTableInterface(tableName);
        Get get = new Get(key);

        HTableInterface htable;
        try {
            htable = getHTableInterface(tableName);
            KVSerializable kvInst = (KVSerializable) clazz.getConstructors()[0].newInstance();
            Result result = htable.get(get);
            if (result != null) {
                kvInst.fromKV(key, result.getValue(CF, V_DATA));
                return kvInst;
            }
        } catch (Exception ex) {
            logger.error("Scan metric meta error, class:{}", clazz.getSimpleName(), ex);
        } finally {
            closeTable(table);
        }
        return null;
    }

    protected void deleteRow(String tableName, byte[] key) {
        HTableInterface table = getHTableInterface(tableName);
        Delete delete = new Delete(key);

        HTableInterface htable;
        try {
            htable = getHTableInterface(tableName);
            htable.delete(delete);
        } catch (Exception ex) {
            logger.error("Failed to delete row, table:{}, key:{}", tableName, key, ex);
        } finally {
            closeTable(table);
        }
    }

    protected ResultScanner getScanner(HTableInterface table, Scan scan) throws IOException {
        return table.getScanner(scan);
    }
}
