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

import com.alibaba.jstorm.common.metric.CounterData;
import com.alibaba.jstorm.common.metric.GaugeData;
import com.alibaba.jstorm.common.metric.HistogramData;
import com.alibaba.jstorm.common.metric.MeterData;
import com.alibaba.jstorm.common.metric.MetricBaseData;
import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.common.metric.TaskTrack;
import com.alibaba.jstorm.common.metric.TopologyHistory;
import com.alibaba.jstorm.metric.KVSerializable;
import com.alibaba.jstorm.metric.MetaFilter;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricQueryClient;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.1.1
 */
@SuppressWarnings("unused")
public class HBaseMetricQueryClient extends AbstractHBaseClient implements MetricQueryClient {

    private final TaskFilter taskFilter = new TaskFilter();
    private final ComponentFilter compFilter = new ComponentFilter();

    private final AtomicBoolean inited = new AtomicBoolean(false);

    @Override
    public synchronized void init(Map conf) {
        if (!inited.get()) {
            initFromStormConf(conf);
            inited.set(true);
        }
    }

    @Override
    public boolean isInited() {
        return inited.get();
    }

    @Override
    public String getIdentity(Map conf) {
        return String.valueOf(conf.get(HBASE_QUORUM_CONF_KEY)) +
                conf.get(HBASE_PORT_CONF_KEY) +
                conf.get(HBASE_ZK_PARENT_CONF_KEY);
    }

    @Override
    public List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type, MetaFilter filter, Object arg) {
        String start = MetricUtils.concat2(clusterName, topologyId, type.getT());
        String end = start + "~";

        List<MetricMeta> rows = scanRows(TABLE_METRIC_META, MetricMeta.class, start.getBytes(), end.getBytes(), MAX_SCAN_META_ROWS);
        return filterMetricMetaList(rows, filter, arg);
    }

    @Override
    public List<MetricMeta> getMetricMeta(String clusterName, String topologyId, MetaType type) {
        return getMetricMeta(clusterName, topologyId, type, null, null);
    }

    @Override
    public List<MetricMeta> getWorkerMeta(String clusterName, String topologyId) {
        return getMetricMeta(clusterName, topologyId, MetaType.WORKER, null, null);
    }

    @Override
    public List<MetricMeta> getNettyMeta(String clusterName, String topologyId) {
        return getMetricMeta(clusterName, topologyId, MetaType.NETTY, null, null);
    }

    @Override
    public List<MetricMeta> getTaskMeta(String clusterName, String topologyId, int taskId) {
        return getMetricMeta(clusterName, topologyId, MetaType.TASK, taskFilter, taskId);
    }

    @Override
    public List<MetricMeta> getComponentMeta(String clusterName, String topologyId, String componentId) {
        return getMetricMeta(clusterName, topologyId, MetaType.COMPONENT, compFilter, componentId);
    }

    @Override
    public MetricMeta getMetricMeta(String clusterName, String topologyId, MetaType metaType, String metricId) {
        String key = MetricUtils.concat2(clusterName, topologyId, metaType.getT(), metricId);
        return getMetricMeta(key);
    }

    @Override
    public MetricMeta getMetricMeta(String key) {
        return (MetricMeta) getRow(TABLE_METRIC_META, MetricMeta.class, key.getBytes());
    }

    @Override
    public List<Object> getMetricData(String metricId, MetricType metricType, int win, long start, long end) {
        return getMetricData(metricId, metricType, win, start, end, 60);
    }

    @Override
    public List<Object> getMetricData(String metricId, MetricType metricType, int win, long start, long end, int size) {
        long metricIdLong = Long.parseLong(metricId);
        byte[] startKey = MetricBaseData.makeKey(metricIdLong, win, start);
        byte[] endKey = MetricBaseData.makeKey(metricIdLong, win, end);

        HTableInterface table = getHTableInterface(TABLE_METRIC_DATA);
        Scan scan = new Scan(startKey, endKey);
        //scan.setBatch(10);
        scan.setCaching(CACHE_SIZE);
        ResultScanner scanner = null;
        try {
            scanner = getScanner(table, scan);
            Iterator<Result> rows = scanner.iterator();
            if (rows != null) {
                List<Object> ret = new ArrayList<>(size);
                while (rows.hasNext()) {
                    Result row = rows.next();
                    Object obj = parseMetricDataRow(row, metricType);
                    if (obj != null) {
                        ret.add(obj);
                    }
                }
                return ret;
            }
        } catch (Exception ex) {
            logger.error("Scan error, metric id:{}, metric type:{}", metricId, metricType, ex);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            closeTable(table);
        }

        return new ArrayList<>(0);
    }

    private Object parseMetricDataRow(Result result, MetricType metricType) {
        KVSerializable serializable;
        if (metricType == MetricType.COUNTER) {
            serializable = new CounterData();
        } else if (metricType == MetricType.GAUGE) {
            serializable = new GaugeData();
        } else if (metricType == MetricType.METER) {
            serializable = new MeterData();
        } else if (metricType == MetricType.HISTOGRAM) {
            serializable = new HistogramData();
        } else {
            return null;
        }
        return serializable.fromKV(result.getRow(), result.getValue(CF, V_DATA));
    }


    @Override
    public List<TaskTrack> getTaskTrack(String clusterName, String topologyId) {
        String start = MetricUtils.concat2(clusterName, topologyId, "");
        String end = start + "~";
        int maxTaskTrack = 10000;

        return scanRows(TABLE_TASK_TRACK, TaskTrack.class, start.getBytes(), end.getBytes(), maxTaskTrack);
    }

    @Override
    public List<TaskTrack> getTaskTrack(String clusterName, String topologyId, int taskId) {
        String start = MetricUtils.concat2(clusterName, topologyId, taskId, "");
        String end = start + "~";

        return scanRows(TABLE_TASK_TRACK, TaskTrack.class, start.getBytes(), end.getBytes(), MAX_SCAN_ROWS);
    }

    @Override
    public List<TopologyHistory> getTopologyHistory(String clusterName, String topologyName, int size) {
        String start = MetricUtils.concat2(clusterName, topologyName);
        // the key is: clusterName@topologyName@time, if we don't add @ after topology name
        // we may scan topologies with the same topology name prefix, e.g., we want to search
        // SeqTest, but we may get results for SeqTest2
        start += MetricUtils.DELIM;

        String end = start + "~";

        int maxSize = 1000;
        if (size > maxSize) {
            size = maxSize;
        }

        List<TopologyHistory> rows = scanRows(
                TABLE_TOPOLOGY_HISTORY, TopologyHistory.class, start.getBytes(), end.getBytes(), size);
        return extractTopologyHistory(rows);
    }

    private List<TopologyHistory> extractTopologyHistory(List<TopologyHistory> rows) {
        Map<String, TopologyHistory> maps = new HashMap<>();

        for (TopologyHistory row : rows) {
            String key = row.getIdentity();
            if (maps.containsKey(key)) {
                maps.get(key).merge(row);
            } else {
                maps.put(key, row);
            }
        }

        // sort by start time
        List<TopologyHistory> historyList = new ArrayList<>(maps.values());
        Collections.sort(historyList, new Comparator<TopologyHistory>() {
            @Override
            public int compare(TopologyHistory o1, TopologyHistory o2) {
                Date t1 = o1.getStart();
                if (t1 == null) {
                    t1 = o1.getEnd();
                }
                Date t2 = o2.getStart();
                if (t2 == null) {
                    t2 = o2.getEnd();
                }
                try {
                    return t1.compareTo(t2);
                } catch (Exception ex) {
                    return -1;
                }
            }
        });

        return historyList;
    }


    @Override
    public void deleteMeta(MetricMeta meta) {
        deleteRow(TABLE_METRIC_META, meta.getKey());
    }

    @Override
    public void deleteMeta(List<MetricMeta> metaList) {
        for (MetricMeta meta : metaList) {
            deleteMeta(meta);
        }
    }

    private List<MetricMeta> filterMetricMetaList(List<MetricMeta> rows, MetaFilter filter, Object arg) {
        List<MetricMeta> ret = new ArrayList<>(MAX_SCAN_ROWS);
        for (MetricMeta row : rows) {
            if (filter == null || filter.matches(row, arg)) {
                ret.add(row);
            }
        }

        return ret;
    }

    public static class TaskFilter implements MetaFilter {
        @Override
        public boolean matches(MetricMeta meta, Object arg) {
            return meta.getTaskId() == (Integer) arg;
        }
    }

    public static class ComponentFilter implements MetaFilter {
        @Override
        public boolean matches(MetricMeta meta, Object arg) {
            return meta.getComponent().equals(arg);
        }
    }
}
