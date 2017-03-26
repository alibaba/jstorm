package com.alibaba.jstorm.task.master.metrics;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.error.ErrorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.metric.MetricsRegister;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMasterContext;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MetricRegister implements TMHandler {
    static private final Logger LOG = LoggerFactory.getLogger(MetricRegister.class);

    private MetricsRegister metricsRegister;
    private TopologyMasterContext tmContext;

    private StormClusterState zkCluster;
    private TopologyContext context;
    private final AtomicBoolean pending = new AtomicBoolean(false);
    private final AtomicReference<Set<String>> metricNames = new AtomicReference<Set<String>>();

    @Override
    public void init(TopologyMasterContext tmContext) {
        this.tmContext = tmContext;
        this.zkCluster = tmContext.getZkCluster();
        this.context = tmContext.getContext();
        this.metricsRegister = new MetricsRegister(tmContext.getConf(), tmContext.getTopologyId());
        metricNames.set(new HashSet<String>());
    }

    @Override
    public void process(Object event) throws Exception {
        if (event instanceof Tuple) {
            registerMetrics((Tuple) event);
        } else if (event instanceof MetricsMetaBroadcastEvent) {
            broadcast();
        } else {
            zkCluster.report_task_error(context.getTopologyId(), context.getThisTaskId(), "Unknown event", ErrorConstants.WARN, ErrorConstants.CODE_USER);
            throw new RuntimeException("Unknown event");
        }
    }

    @Override
    public void cleanup() {
    }

    private void registerMetrics(Tuple input) {
        synchronized (metricNames) {
            metricNames.get().addAll((Set<String>) input.getValue(0));
        }
    }

    public void broadcast() {
        if (pending.compareAndSet(false, true)) {
            try {
                Set<String> oldMetricsNames = metricNames.getAndSet(new HashSet<String>());
                if (oldMetricsNames.size() > 0) {
                    LOG.debug("register metrics to nimbus from TM, size:{}", oldMetricsNames.size());
                    Map<String, Long> nameIdMap = metricsRegister.registerMetrics(oldMetricsNames);
                    LOG.debug("register metrics to nimbus from TM, ret size:{}", nameIdMap.size());
                    if (nameIdMap.size() > 0) {
                        // we broadcast metrics meta to all workers, for large
                        // topologies, might be quite large
                        for (ResourceWorkerSlot worker : tmContext.getWorkerSet().get()) {
                            Set<Integer> tasks = worker.getTasks();
                            int task = tasks.iterator().next();
                            tmContext.getCollector().getDelegate().emitDirect(task,
                                    Common.TOPOLOGY_MASTER_REGISTER_METRICS_RESP_STREAM_ID,
                                    null, new Values(nameIdMap));
                        }
                    }
                }
            } catch (Throwable e) {
                LOG.error("Error:", e);
            } finally {
                pending.set(false);
            }
        } else {
            LOG.warn("pending register metrics, skip...");
        }
    }

}
