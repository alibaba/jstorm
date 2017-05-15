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

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmMeter;
import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.common.metric.TaskTrack;
import com.alibaba.jstorm.common.metric.TopologyHistory;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.KillTopologyEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.StartTopologyEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.TaskDeadEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.TaskStartEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.KVSerializable;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricDataConverter;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.TimeTicker;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.1.1
 */
@SuppressWarnings("unused")
public class HBaseMetricSendClient extends AbstractHBaseClient implements MetricUploader {

    private Map conf;
    private ClusterMetricsRunnable metricsRunnable;

    // metrics
    protected final AsmMeter hbaseSendTps;
    protected final AsmCounter succCounter;
    protected final AsmCounter failedCounter;

    protected final MetricUploadFilter metricUploadFilter = new MetricUploadFilter();

    private final ExecutorService hbaseInsertThreadPool = Executors.newFixedThreadPool(20, new ThreadFactory() {
        private final String group = "HBaseInsertThread-";
        private final AtomicLong id = new AtomicLong(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(group + id.getAndIncrement());
            return thread;
        }
    });

    public HBaseMetricSendClient() {
        this.hbaseSendTps = JStormMetrics.registerWorkerMeter(
                JStormMetrics.NIMBUS_METRIC_KEY, "HBaseSendTps", new AsmMeter());
        this.succCounter = JStormMetrics.registerWorkerCounter(
                JStormMetrics.NIMBUS_METRIC_KEY, "TotalHBaseInsert", new AsmCounter());
        this.failedCounter = JStormMetrics.registerWorkerCounter(
                JStormMetrics.NIMBUS_METRIC_KEY, "HBaseFailed", new AsmCounter());
    }

    @Override
    public void init(NimbusData nimbusData) throws Exception {
        this.conf = nimbusData.getConf();
        metricUploadFilter.parseInsertFilters(conf, NIMBUS_CONF_PREFIX + "hbase" + INSERT_FILTER_SUFFIX);

        this.metricsRunnable = ClusterMetricsRunnable.getInstance();
        initFromStormConf(conf);
    }

    @Override
    public void cleanup() {
        try {
            hTablePool.close();
        } catch (Exception ignored) {
        }
        hbaseInsertThreadPool.shutdown();
    }

    @Override
    public boolean registerMetrics(String clusterName, String topologyId, Map<String, Long> metricMeta) throws Exception {
        List<KVSerializable> metaList = new ArrayList<>(metricMeta.size());
        for (Map.Entry<String, Long> entry : metricMeta.entrySet()) {
            MetricMeta meta = MetricMeta.parse(entry.getKey());
            meta.setClusterName(clusterName);
            meta.setId(entry.getValue());
            metaList.add(meta);
        }
        return batchAdd(metaList, TABLE_METRIC_META);
    }

    @Override
    public boolean upload(final String clusterName, final String topologyId,
                          final TopologyMetric tpMetric, final Map<String, Object> metricContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean upload(final String clusterName, final String topologyId,
                          final Object key, final Map<String, Object> metricContext) {
        final Integer idx = (Integer) key;
        final String type = (String) metricContext.get(MetricUploader.METRIC_TYPE);
        final Long timestamp = (Long) metricContext.get(MetricUploader.METRIC_TIME);

        if (StringUtils.isBlank(topologyId) || StringUtils.isBlank(type) || timestamp == null) {
            logger.error("Invalid parameter, clusterName:{}, topologyId:{}, metricContext:{}",
                    clusterName, topologyId, metricContext);
            metricsRunnable.getContext().markUploaded(HBaseMetricSendClient.this, idx);
            return false;
        }

        final Date time = new Date(timestamp);
        if (topologyId.equals(JStormMetrics.NIMBUS_METRIC_KEY)) {
            hbaseInsertThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    int size = 0;
                    long cost = 0;
                    TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);
                    try {
                        TopologyMetric allMetrics = metricsRunnable.getContext().getMetricDataFromCache(idx);
                        if (allMetrics != null && allMetrics.get_workerMetric().get_metrics_size() > 0) {
                            size += allMetrics.get_workerMetric().get_metrics_size();
                            sendMetricInfo(topologyId, allMetrics.get_workerMetric(), MetaType.WORKER, time);
                        }
                        cost = ticker.stop();
                    } catch (Throwable ex) {
                        logger.error("Error", ex);
                    } finally {
                        metricsRunnable.getContext().markUploaded(HBaseMetricSendClient.this, idx);
                        logger.info("insert topology metrics for topology:{}, type:{}, time:{}, total:{}, cost:{}",
                                topologyId, type, TimeUtils.toTimeStr(new Date(timestamp)), size, cost);
                    }
                }
            });
        } else {
            if (MetricUploader.METRIC_TYPE_ALL.equals(type)) {
                hbaseInsertThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        int size = 0;
                        long cost = 0;
                        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);
                        try {
                            TopologyMetric allMetrics = metricsRunnable.getContext().getMetricDataFromCache(idx);
                            if (allMetrics == null) {
                                return;
                            }

                            final MetricInfo topologyMetrics = allMetrics.get_topologyMetric();
                            final MetricInfo compMetrics = allMetrics.get_componentMetric();
                            final MetricInfo compStreamMetrics = allMetrics.get_compStreamMetric();
                            final MetricInfo taskMetrics = allMetrics.get_taskMetric();
                            final MetricInfo streamMetrics = allMetrics.get_streamMetric();
                            final MetricInfo nettyMetrics = allMetrics.get_nettyMetric();
                            final MetricInfo workerMetrics = allMetrics.get_workerMetric();

                            if (topologyMetrics.get_metrics_size() > 0) {
                                size += topologyMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, topologyMetrics, MetaType.TOPOLOGY, time);
                            }
                            if (compMetrics.get_metrics_size() > 0) {
                                size += compMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, compMetrics, MetaType.COMPONENT, time);
                            }
                            if (compStreamMetrics != null && compStreamMetrics.get_metrics_size() > 0) {
                                size += compStreamMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, compStreamMetrics, MetaType.COMPONENT_STREAM, time);
                            }
                            if (taskMetrics.get_metrics_size() > 0) {
                                size += taskMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, taskMetrics, MetaType.TASK, time);
                            }
                            if (streamMetrics.get_metrics_size() > 0) {
                                size += streamMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, streamMetrics, MetaType.STREAM, time);
                            }
                            if (nettyMetrics.get_metrics_size() > 0) {
                                size += nettyMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, nettyMetrics, MetaType.NETTY, time);
                            }
                            if (workerMetrics.get_metrics_size() > 0) {
                                size += workerMetrics.get_metrics_size();
                                sendMetricInfo(topologyId, workerMetrics, MetaType.WORKER, time);
                            }
                            cost = ticker.stop();
                        } catch (Throwable t) { // use throwable as we may encounter NoClassDef errors.
                            logger.error("Error", t);
                        } finally {
                            metricsRunnable.getContext().markUploaded(HBaseMetricSendClient.this, idx);
                            logger.info("insert topology metrics for topology:{}, type:{}, time:{}, total:{}, cost:{}",
                                    topologyId, type, TimeUtils.toTimeStr(new Date(timestamp)), size, cost);
                        }
                    }
                });
            } else if (MetricUploader.METRIC_TYPE_TOPLOGY.equals(type)) {
                hbaseInsertThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        int size = 0;
                        long cost = 0;
                        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);
                        try {
                            TopologyMetric allMetrics = metricsRunnable.getContext().getMetricDataFromCache(idx);
                            if (allMetrics != null) {
                                final MetricInfo topologyMetrics = allMetrics.get_topologyMetric();
                                final MetricInfo compMetrics = allMetrics.get_componentMetric();
                                final MetricInfo compStreamMetrics = allMetrics.get_compStreamMetric();

                                if (topologyMetrics.get_metrics_size() > 0) {
                                    size += topologyMetrics.get_metrics_size();
                                    sendMetricInfo(topologyId, topologyMetrics, MetaType.TOPOLOGY, time);
                                }
                                if (compMetrics.get_metrics_size() > 0) {
                                    size += compMetrics.get_metrics_size();
                                    sendMetricInfo(topologyId, compMetrics, MetaType.COMPONENT, time);
                                }
                                if (compStreamMetrics != null && compStreamMetrics.get_metrics_size() > 0) {
                                    size += compStreamMetrics.get_metrics_size();
                                    sendMetricInfo(topologyId, compStreamMetrics, MetaType.COMPONENT_STREAM, time);
                                }
                            }
                            cost = ticker.stop();
                        } catch (Throwable ex) {
                            logger.error("Error", ex);
                        } finally {
                            metricsRunnable.getContext().markUploaded(HBaseMetricSendClient.this, idx);
                            logger.info("insert topology metrics for topology:{}, type:{}, time:{}, total:{}, cost:{}",
                                    topologyId, type, TimeUtils.toTimeStr(new Date(timestamp)), size, cost);
                        }
                    }
                });
            } else {
                hbaseInsertThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        int size = 0;
                        long cost = 0;
                        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);
                        try {
                            TopologyMetric allMetrics = metricsRunnable.getContext().getMetricDataFromCache(idx);
                            if (allMetrics != null) {
                                MetricInfo[] metricInfoList = new MetricInfo[]{
                                        allMetrics.get_taskMetric(),
                                        allMetrics.get_streamMetric(),
                                        allMetrics.get_workerMetric(),
                                        allMetrics.get_nettyMetric()};
                                MetaType[] metaTypes = new MetaType[]{
                                        MetaType.TASK, MetaType.STREAM, MetaType.WORKER, MetaType.NETTY};
                                for (int i = 0; i < metricInfoList.length; i++) {
                                    final MetricInfo metricInfo = metricInfoList[i];
                                    if (metricInfo.get_metrics_size() > 0) {
                                        size += metricInfo.get_metrics_size();
                                        sendMetricInfo(topologyId, metricInfo, metaTypes[i], time);
                                    }
                                }
                            }
                            cost = ticker.stop();
                        } catch (Throwable ex) {
                            logger.error("Error", ex);
                        } finally {
                            metricsRunnable.getContext().markUploaded(HBaseMetricSendClient.this, idx);
                            logger.info("insert topology metrics for topology:{}, type:{}, time:{}, total:{}, cost:{}",
                                    topologyId, type, TimeUtils.toTimeStr(new Date(timestamp)), size, cost);
                        }
                    }
                });
            }
        }

        return true;
    }

    private void sendMetricInfo(final String topologyId, final MetricInfo metricInfo,
                                final MetaType metaType, final Date time) {
        if (metricUploadFilter.filter(metaType)) {
            return;
        }

        int cnt = 0;
        int BATCH = 100;

        logger.info("HBase insert thread, topology:{}, type:{}, time:{}, size:{}",
                topologyId, metaType, TimeUtils.toTimeStr(time), metricInfo.get_metrics_size());
        List<KVSerializable> metricDataList = new ArrayList<>(BATCH);
        try {
            Map<String, Map<Integer, MetricSnapshot>> metrics = metricInfo.get_metrics();
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metrics.entrySet()) {
                Map<Integer, MetricSnapshot> metric = metricEntry.getValue();
                for (Map.Entry<Integer, MetricSnapshot> entry : metric.entrySet()) {
                    Integer win = entry.getKey();
                    MetricSnapshot snapshot = entry.getValue();
                    if (MetricUtils.isValidId(snapshot.get_metricId())) {
                        MetricType metricType = MetricType.parse(snapshot.get_metricType());
                        if (metricType == MetricType.COUNTER) {
                            metricDataList.add(MetricDataConverter.toCounterData(snapshot, win));
                        } else if (metricType == MetricType.GAUGE) {
                            metricDataList.add(MetricDataConverter.toGaugeData(snapshot, win));
                        } else if (metricType == MetricType.METER) {
                            metricDataList.add(MetricDataConverter.toMeterData(snapshot, win));
                        } else if (metricType == MetricType.HISTOGRAM) {
                            metricDataList.add(MetricDataConverter.toHistogramData(snapshot, win));
                        }

                        cnt++;
                    }
                    if (cnt >= BATCH) {
                        batchAdd(metricDataList, TABLE_METRIC_DATA);
                        cnt = 0;
                        metricDataList.clear();
                    }
                }
            }

            if (cnt > 0) {
                batchAdd(metricDataList, TABLE_METRIC_DATA);
            }
        } catch (Throwable ex) {
            logger.error("transaction error", ex);
        }
    }

    @Override
    public boolean sendEvent(String clusterName, MetricEvent inputEvent) {
        if (inputEvent instanceof StartTopologyEvent) {
            StartTopologyEvent event = (StartTopologyEvent) inputEvent;
            logger.info("starting topology: cluster {}, topology {} at {}, metric sample rate:{}",
                    event.getClusterMetricsContext().getClusterName(),
                    event.getTopologyId(), event.getTimestamp(), event.getSampleRate());

            insertTopologyHistory(event);
        } else if (inputEvent instanceof KillTopologyEvent) {
            KillTopologyEvent event = (KillTopologyEvent) inputEvent;
            logger.info("killing topology: cluster {}, topology {} at {}",
                    event.getClusterMetricsContext().getClusterName(),
                    event.getTopologyId(), event.getTimestamp());

            updateTopology(event);
        } else if (inputEvent instanceof TaskDeadEvent) {
            TaskDeadEvent event = (TaskDeadEvent) inputEvent;
            logger.info("tasks are dead on cluster {}, topology {}, tasks: {}",
                    event.getClusterMetricsContext().getClusterName(),
                    event.getTopologyId(), event.getDeadTasks().toString());

            updateTasks(event);
        } else if (inputEvent instanceof TaskStartEvent) { // topology rebalance will send this event too.
            TaskStartEvent event = (TaskStartEvent) inputEvent;
            logger.info("task starts on cluster {}, topology {}",
                    event.getClusterMetricsContext().getClusterName(),
                    event.getTopologyId());

            insertOrUpdateTasks(event);
        } else {
            logger.error("Unknown event {} of {}", inputEvent, clusterName);
        }

        return true;
    }

    /**
     * insert tasks on new assign, update tasks on rebalance.
     */
    private void insertOrUpdateTasks(TaskStartEvent event) {
        Assignment old = event.getOldAssignment();
        Assignment current = event.getNewAssignment();

        Map<Integer, String> task2Component = event.getTask2Component();
        List<KVSerializable> taskTrackList = new ArrayList<>();

        // assign
        if (old == null) {
            Set<ResourceWorkerSlot> workers = current.getWorkers();
            logger.info("old workers are null, assigned workers:{}", Joiner.on(",").join(workers));
            for (ResourceWorkerSlot worker : workers) {
                Set<Integer> tasks = worker.getTasks();
                for (Integer task : tasks) {
                    TaskTrack track = new TaskTrack(
                            event.getClusterMetricsContext().getClusterName(), event.getTopologyId());
                    track.setStart(new Date(event.getTimestamp()));
                    track.setComponent(task2Component.get(task));
                    track.setHost(worker.getHostname());
                    track.setPort(worker.getPort());
                    track.setTaskId(task);

                    taskTrackList.add(track);
                }
            }
        } else { // rebalance, we only insert newly assigned tasks
            Set<ResourceWorkerSlot> oldWorkers = old.getWorkers();
            Joiner joiner = Joiner.on(",");
            logger.info("old workers:{}, new workers:{}", joiner.join(oldWorkers), joiner.join(current.getWorkers()));
            for (ResourceWorkerSlot worker : current.getWorkers()) {
                // a new worker, insert all tasks
                if (!oldWorkers.contains(worker)) {
                    for (Integer task : worker.getTasks()) {
                        TaskTrack track = new TaskTrack(
                                event.getClusterMetricsContext().getClusterName(),
                                event.getTopologyId());
                        track.setStart(new Date(event.getTimestamp()));
                        track.setComponent(task2Component.get(task));
                        track.setHost(worker.getHostname());
                        track.setPort(worker.getPort());
                        track.setTaskId(task);

                        taskTrackList.add(track);
                    }
                } else {
                    for (Integer task : worker.getTasks()) {
                        ResourceWorkerSlot oldWorker = old.getWorkerByTaskId(task);
                        if (oldWorker != null) {

                            // update end time of old task
                            TaskTrack oldTrack = new TaskTrack(
                                    event.getClusterMetricsContext().getClusterName(),
                                    event.getTopologyId());
                            oldTrack.setEnd(new Date(event.getTimestamp()));
                            oldTrack.setTaskId(task);
                            oldTrack.setHost(oldWorker.getHostname());
                            oldTrack.setPort(oldWorker.getPort());
                            taskTrackList.add(oldTrack);

                            // insert new task
                            TaskTrack track = new TaskTrack(
                                    event.getClusterMetricsContext().getClusterName(),
                                    event.getTopologyId());
                            track.setStart(new Date());
                            track.setComponent(task2Component.get(task));
                            track.setHost(worker.getHostname());
                            track.setPort(worker.getPort());
                            track.setTaskId(task);
                            taskTrackList.add(track);
                        }
                    }
                }
            }
        }
        if (taskTrackList.size() > 0) {
            batchAdd(taskTrackList, TABLE_TASK_TRACK);
        }
    }

    private void updateTasks(TaskDeadEvent event) {
        Map<Integer, ResourceWorkerSlot> deadTasks = event.getDeadTasks();

        List<KVSerializable> taskTrackList = new ArrayList<>(deadTasks.size());
        for (Map.Entry<Integer, ResourceWorkerSlot> task : deadTasks.entrySet()) {
            TaskTrack taskTrack = new TaskTrack(
                    event.getClusterMetricsContext().getClusterName(),
                    event.getTopologyId());
            taskTrack.setEnd(new Date(event.getTimestamp()));
            taskTrack.setTaskId(task.getKey());
            taskTrack.setHost(task.getValue().getHostname());
            taskTrack.setPort(task.getValue().getPort());

            taskTrackList.add(taskTrack);
        }
        batchAdd(taskTrackList, TABLE_TASK_TRACK);
    }

    private void insertTopologyHistory(StartTopologyEvent event) {
        TopologyHistory history = new TopologyHistory(
                event.getClusterMetricsContext().getClusterName(),
                event.getTopologyId());
        history.setTopologyName(Common.getTopologyNameById(event.getTopologyId()));
        history.setStart(new Date(event.getTimestamp()));
        history.setSampleRate(event.getSampleRate());

        add(history, TABLE_TOPOLOGY_HISTORY);
    }

    private void updateTopology(KillTopologyEvent event) {
        TopologyHistory history = new TopologyHistory(
                event.getClusterMetricsContext().getClusterName(),
                event.getTopologyId());
        history.setTopologyName(Common.getTopologyNameById(event.getTopologyId()));
        history.setEnd(new Date(event.getTimestamp()));

        add(history, TABLE_TOPOLOGY_HISTORY);
    }


    protected boolean batchAdd(Collection<KVSerializable> items, String tableName) {
        int size = items.size();
        List<Put> batch = new ArrayList<>(size);
        HTableInterface table = getHTableInterface(tableName);
        for (KVSerializable v : items) {
            byte[] rowKey = v.getKey();
            Put put = new Put(rowKey);
            put.add(CF, V_DATA, v.getValue());
            batch.add(put);
        }
        try {
            table.put(batch);
            table.flushCommits();
            succCounter.update(size);
            hbaseSendTps.update(size);
        } catch (Throwable ex) {
            logger.error("Error", ex);
            failedCounter.update(size);
            return false;
        } finally {
            closeTable(table);
        }
        return true;
    }

    protected boolean add(KVSerializable item, String tableName) {
        long size = 1L;

        Put put = new Put(item.getKey());
        HTableInterface table = getHTableInterface(tableName);

        //put.setWriteToWAL(false);
        put.add(CF, V_DATA, item.getValue());
        try {
            table.put(put);
            table.flushCommits();
            succCounter.update(size);
            hbaseSendTps.update(size);
        } catch (Throwable ex) {
            logger.error("Error", ex);
            failedCounter.update(size);
            return false;
        } finally {
            closeTable(table);
        }
        return true;
    }


}
