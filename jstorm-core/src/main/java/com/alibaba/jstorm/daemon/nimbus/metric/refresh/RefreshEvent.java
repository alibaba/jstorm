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
package com.alibaba.jstorm.daemon.nimbus.metric.refresh;

import com.alibaba.jstorm.daemon.nimbus.metric.CheckMetricEvent;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.RemoveTopologyEvent;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.TimeTicker;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.google.common.collect.Sets;

/**
 * Sync meta <String, Long> from cache and remote
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 */
public class RefreshEvent extends MetricEvent {
    private static final Logger LOG = LoggerFactory.getLogger(RefreshEvent.class);

    /**
     * we sync meta from remote when nimbus uptime < 600 sec
     * in the future when nimbus can be dynamically updated,
     * we should automatically send an RefreshEvent on update to
     * trigger the sync
     */
    private static final int INIT_SYNC_REMOTE_META_TIME_SEC = 10 * 60;


    private static final int SYNC_REMOTE_META_TIME_SEC = 5 * 60;
    /**
     * after initial sync time, sync remote every 5 min
     */
    private static final ConcurrentMap<String, Integer> syncRemoteTimes = new ConcurrentHashMap<>();

    @Override
    public void run() {
        refreshTopologies();
    }

    /**
     * refresh metric settings of topologies & metric meta
     */
    public void refreshTopologies() {
        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);
        try {
            doRefreshTopologies();
            LOG.debug("Refresh topologies, cost:{}", ticker.stopAndRestart());

            if (!context.getNimbusData().isLeader()) { // won't hit...
                syncTopologyMetaForFollower();
                LOG.debug("Sync topology meta, cost:{}", ticker.stop());
            }
        } catch (Exception ex) {
            LOG.error("handleRefreshEvent error:", ex);
        }
    }

    /**
     * refresh metric settings of topologies and sync metric meta from local cache
     */
    @SuppressWarnings("unchecked")
    private void doRefreshTopologies() {
        for (String topology : JStormMetrics.SYS_TOPOLOGIES) {
            if (!context.getTopologyMetricContexts().containsKey(topology)) {
                LOG.info("adding {} to metric context.", topology);
                Map conf = new HashMap();
                if (topology.equals(JStormMetrics.CLUSTER_METRIC_KEY)) {
                    //there's no need to consider sample rate when cluster metrics merge
                    conf.put(ConfigExtension.TOPOLOGY_METRIC_SAMPLE_RATE, 1.0);
                }
                Set<ResourceWorkerSlot> workerSlot = Sets.newHashSet(new ResourceWorkerSlot());
                TopologyMetricContext metricContext = new TopologyMetricContext(topology, workerSlot, conf);
                context.getTopologyMetricContexts().putIfAbsent(topology, metricContext);
                syncMetaFromCache(topology, context.getTopologyMetricContexts().get(topology));
            }

            // in the first 10 min, sync every 1 min
            if (context.getNimbusData().uptime() < INIT_SYNC_REMOTE_META_TIME_SEC) {
                syncMetaFromRemote(topology, context.getTopologyMetricContexts().get(topology),
                        Lists.newArrayList(MetaType.TOPOLOGY, MetaType.WORKER, MetaType.NIMBUS));
            } else { // after that, sync every 5 min
                checkTimeAndSyncMetaFromRemote(topology, context.getTopologyMetricContexts().get(topology),
                        Lists.newArrayList(MetaType.values()));

            }
        }

        Map<String, Assignment> assignMap;
        try {
            assignMap = Cluster.get_all_assignment(context.getStormClusterState(), null);
            for (Entry<String, Assignment> entry : assignMap.entrySet()) {
                String topology = entry.getKey();
                Assignment assignment = entry.getValue();

                TopologyMetricContext metricContext = context.getTopologyMetricContexts().get(topology);
                if (metricContext == null) {
                    metricContext = new TopologyMetricContext(assignment.getWorkers());
                    metricContext.setTaskNum(NimbusUtils.getTopologyTaskNum(assignment));
                    syncMetaFromCache(topology, metricContext);

                    LOG.info("adding {} to metric context.", topology);
                    context.getTopologyMetricContexts().put(topology, metricContext);
                } else {
                    boolean modify = false;
                    if (metricContext.getTaskNum() != NimbusUtils.getTopologyTaskNum(assignment)) {
                        modify = true;
                        metricContext.setTaskNum(NimbusUtils.getTopologyTaskNum(assignment));
                    }

                    if (!assignment.getWorkers().equals(metricContext.getWorkerSet())) {
                        modify = true;
                        metricContext.setWorkerSet(assignment.getWorkers());
                    }

                    // for normal topologies, only sync topology/component level metrics
                    checkTimeAndSyncMetaFromRemote(topology, metricContext,
                            Lists.newArrayList(MetaType.TOPOLOGY, MetaType.COMPONENT));

                    // we may need to sync meta when task num/workers change
                    metricContext.setSyncMeta(!modify);
                }
            }
        } catch (Exception e1) {
            LOG.warn("Failed to get assignments");
            return;
        }

        List<String> removing = new ArrayList<>();
        for (String topology : context.getTopologyMetricContexts().keySet()) {
            if (!JStormMetrics.SYS_TOPOLOGY_SET.contains(topology) && !assignMap.containsKey(topology)) {
                removing.add(topology);
            }
        }

        for (String topology : removing) {
            LOG.info("removing topology:{}", topology);
            RemoveTopologyEvent.pushEvent(topology);
            syncRemoteTimes.remove(topology);
        }
    }

    private void checkTimeAndSyncMetaFromRemote(String topology, TopologyMetricContext metricContext,
                                                List<MetaType> metaTypes) {
        Integer curTimeSec = TimeUtils.current_time_secs();

        // for normal topologies, check topology/component level metrics
        Integer lastCheck = syncRemoteTimes.get(topology);
        if (lastCheck != null && curTimeSec - lastCheck >= SYNC_REMOTE_META_TIME_SEC) {
            syncRemoteTimes.put(topology, curTimeSec);
            syncMetaFromRemote(topology, metricContext, metaTypes);
        }
    }

    /**
     * sync topology metric meta from external storage like TDDL/OTS.
     * nimbus server will skip syncing, only followers do this
     */
    public void syncTopologyMetaForFollower() {
        // sys meta, use remote only
        syncSysMetaFromRemote();

        // normal topology meta, local + remote
        for (Entry<String, TopologyMetricContext> entry : context.getTopologyMetricContexts().entrySet()) {
            String topology = entry.getKey();
            TopologyMetricContext metricContext = entry.getValue();

            if (!JStormMetrics.SYS_TOPOLOGY_SET.contains(topology)) {
                try {
                    syncMetaFromCache(topology, metricContext);
                    syncNonSysMetaFromRemote(topology, metricContext);
                } catch (Exception e1) {
                    LOG.warn("failed to sync meta for topology:{}", topology);
                }
            }
        }
    }

    /**
     * sync metric meta from rocks db into mem cache on startup
     */
    private void syncMetaFromCache(String topology, TopologyMetricContext tmContext) {
        if (!tmContext.syncMeta()) {
            Map<String, Long> meta = context.getMetricCache().getMeta(topology);
            if (meta != null) {
                tmContext.getMemMeta().putAll(meta);
            }
            tmContext.setSyncMeta(true);
        }
    }

    /**
     * sync sys topologies from remote because we want to keep all historic metric data
     * thus metric id cannot be changed.
     */
    private void syncSysMetaFromRemote() {
        for (String topology : JStormMetrics.SYS_TOPOLOGIES) {
            if (context.getTopologyMetricContexts().containsKey(topology)) {
                syncMetaFromRemote(topology,
                        context.getTopologyMetricContexts().get(topology),
                        Lists.newArrayList(MetaType.TOPOLOGY, MetaType.WORKER, MetaType.NIMBUS));
            }
        }
    }

    private void syncNonSysMetaFromRemote(String topology, TopologyMetricContext tmContext) {
        syncMetaFromRemote(topology, tmContext, Lists.newArrayList(MetaType.values()));
    }

    private void syncMetaFromRemote(String topology, TopologyMetricContext tmContext, List<MetaType> metaTypes) {
        LOG.debug("sync meta from remote, topology:{}", topology);
        try {
            int memSize = tmContext.getMemMeta().size();
            //Integer zkSize = (Integer) context.getStormClusterState().get_topology_metric(topology);

            Set<String> added = new HashSet<>();
            List<Pair<MetricMeta, Long>> pairsToCheck = new ArrayList<>();

            ConcurrentMap<String, Long> memMeta = tmContext.getMemMeta();
            for (MetaType metaType : metaTypes) {
                List<MetricMeta> metaList = context.getMetricQueryClient().getMetricMeta(context.getClusterName(),
                        topology, metaType);
                if (metaList != null) {
                    LOG.debug("get remote metric meta, topology:{}, metaType:{}, local mem:{}, remote:{}",
                            topology, metaType, memSize, metaList.size());
                    for (MetricMeta meta : metaList) {
                        String fqn = meta.getFQN();
                        if (added.contains(fqn)) {
                            Long existingId = memMeta.get(fqn);
                            if (existingId != null && existingId != meta.getId()) {
                                LOG.warn("duplicate remote metric meta:{}, will double-check...", fqn);
                                pairsToCheck.add(new Pair<>(meta, existingId));
                            }
                        } else { // force remote to overwrite local meta
                            LOG.debug("overwrite local from remote:{}", fqn);
                            added.add(fqn);
                            memMeta.put(fqn, meta.getId());
                        }
                    }
                }
            }
            context.getMetricCache().putMeta(topology, memMeta);
            if (pairsToCheck.size() > 0) {
                CheckMetricEvent.pushEvent(topology, tmContext, pairsToCheck);
            }
        } catch (Exception ex) {
            LOG.error("failed to sync remote meta", ex);
        }
    }

}
