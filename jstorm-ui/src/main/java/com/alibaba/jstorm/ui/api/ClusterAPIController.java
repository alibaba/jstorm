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
package com.alibaba.jstorm.ui.api;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.ui.model.*;
import com.alibaba.jstorm.ui.model.graph.ChartSeries;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIDef;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@RestController
@RequestMapping(UIDef.API_V2 + "/cluster/{clusterName}")
public class ClusterAPIController {
    private final static Logger LOG = LoggerFactory.getLogger(ClusterAPIController.class);

    @RequestMapping("/summary")
    public ClusterEntity summary(@PathVariable String clusterName){
        UIUtils.readUiConfig();
        return UIUtils.clustersCache.get(clusterName);
    }

    @RequestMapping("/metrics")
    public Map metrics(@PathVariable String clusterName) {
        int window = 60;    //we only get the 60s window
        Map ret;
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            List<MetricInfo> infos = client.getClient().getMetrics(JStormMetrics.CLUSTER_METRIC_KEY,
                    MetaType.TOPOLOGY.getT());
            List<ChartSeries> metrics = UIUtils.getChartSeries(infos, window);
            ret = new HashMap<>();
            ret.put("metrics", metrics);
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }

    @RequestMapping("/supervisor/summary")
    public Map supervisors(@PathVariable String clusterName){
        Map ret;
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();
            ret = new HashMap<>();
            ret.put("supervisors", UIUtils.getSupervisorEntities(clusterSummary));
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }

    @RequestMapping("/supervisor/{host:.+}")
    public Map workers(@PathVariable String clusterName, @PathVariable String host,
                       @RequestParam(value = "window", required = false) String window){
        Map<String, Object> ret = new HashMap<>();
        int win = UIUtils.parseWindow(window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            SupervisorWorkers supervisorWorkers = client.getClient().getSupervisorWorkers(host);
            ret.put("supervisor", new SupervisorEntity(supervisorWorkers.get_supervisor()));

            //get worker summary
            List<WorkerSummary> workerSummaries = supervisorWorkers.get_workers();
            ret.put("workers", UIUtils.getWorkerEntities(workerSummaries));

            Map<String, MetricInfo> workerMetricInfo = supervisorWorkers.get_workerMetric();
            List<UIWorkerMetric> workerMetrics = UIMetricUtils.getWorkerMetrics(workerMetricInfo,
                    workerSummaries, host, win);
            ret.put("workerMetrics", workerMetrics);

        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }

    @RequestMapping("/nimbus/summary")
    public Map nimbus(@PathVariable String clusterName){
        Map ret;
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();
            ret = new HashMap<>();
            ret.put("nimbus", UIUtils.getNimbusEntities(clusterSummary));
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }

    @RequestMapping("/zookeeper/summary")
    public Map zookeepers(@PathVariable String clusterName){
        Map ret = new HashMap<>();
        List<ZooKeeperEntity> zkServers = UIUtils.getZooKeeperEntities(clusterName);
        ret.put("zookeepers", zkServers);
        return ret;
    }

    @RequestMapping("/topology/summary")
    public Map topology(@PathVariable String clusterName){
        Map ret = new HashMap<>();
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();
            List<TopologyEntity> topologies = UIUtils.getTopologyEntities(clusterSummary);
            ret.put("topologies", topologies);
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }


}