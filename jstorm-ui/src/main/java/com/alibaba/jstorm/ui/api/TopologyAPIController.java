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
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.ui.model.*;
import com.alibaba.jstorm.ui.model.graph.ChartSeries;
import com.alibaba.jstorm.ui.model.graph.TopologyGraph;
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

import java.util.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@RestController
@RequestMapping(UIDef.API_V2 + "/cluster/{clusterName}/topology/{topology}")
public class TopologyAPIController {
    private final static Logger LOG = LoggerFactory.getLogger(TopologyAPIController.class);

    @RequestMapping("")
    public Map components(@PathVariable String clusterName, @PathVariable String topology,
                          @RequestParam(value = "window", required = false) String window){
        Map<String, Object> ret = new HashMap<>();
        int win = UIUtils.parseWindow(window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topology);
            TopologySummary topologySummary = topologyInfo.get_topology();
            // fill in topology summary information
            ret.put("id", topologySummary.get_id());
            ret.put("name", topologySummary.get_name());
            ret.put("uptime", UIUtils.prettyUptime(topologySummary.get_uptimeSecs()));
            ret.put("uptimeSeconds", topologySummary.get_uptimeSecs());
            ret.put("status", topologySummary.get_status());
            ret.put("tasksTotal", topologySummary.get_numTasks());
            ret.put("workersTotal", topologySummary.get_numWorkers());
            ret.put("windowHint", AsmWindow.win2str(win));

            // fill in topology metrics information
            MetricInfo topologyMetrics = topologyInfo.get_metrics().get_topologyMetric();
            UISummaryMetric topologyData = UIMetricUtils.getSummaryMetrics(topologyMetrics, win);
            ret.put("topologyMetrics", topologyData);

            // fill in components metrics information
            MetricInfo componentMetrics = topologyInfo.get_metrics().get_componentMetric();
            List<UIUserDefinedMetric> userDefinedMetrics = new ArrayList<>();
            List<UIComponentMetric> componentData = UIMetricUtils.getComponentMetrics(componentMetrics, win,
                    topologyInfo.get_components(), userDefinedMetrics);
            ret.put("componentMetrics", componentData);
            ret.put("userDefinedMetrics", userDefinedMetrics);


            // fill in workers metrics information
            MetricInfo workerMetrics = topologyInfo.get_metrics().get_workerMetric();
            List<UIWorkerMetric> workerData = UIMetricUtils.getWorkerMetrics(workerMetrics, topology, win);
            ret.put("workerMetrics", workerData);

            // fill in tasks entities information
            List<TaskEntity> taskData = UIUtils.getTaskEntities(topologyInfo);
            ret.put("taskStats", taskData);


        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }

    @RequestMapping("/graph")
    public Map graph(@PathVariable String clusterName, @PathVariable String topology) {
        Map<String, Object> result = new HashMap<>();
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            StormTopology stormTopology = client.getClient().getTopology(topology);

            int size = componentSize(stormTopology);

            if (size < 100) {
                List<MetricInfo> componentMetrics = client.getClient().getMetrics(topology, MetaType.COMPONENT.getT());

                TopologyGraph graph = UIUtils.getTopologyGraph(stormTopology, componentMetrics);

                result.put("graph", graph);
            } else {
                result.put("error", "too many components, please check your topology first!");
            }
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            result = UIUtils.exceptionJson(e);
        }
        return result;
    }

    private int componentSize(StormTopology stormTopology){
        Map<String, Bolt> bolts = stormTopology.get_bolts();
        Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
        return bolts.size() + spouts.size();
    }

    @RequestMapping("/metrics")
    public Map summaryMetrics(@PathVariable String clusterName, @PathVariable String topology) {
        int window = 60;    //we only get the 60s window
        Map<String, Object> ret = new HashMap<>();
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            List<MetricInfo> infos = client.getClient().getMetrics(topology, MetaType.TOPOLOGY.getT());
            List<ChartSeries> metrics = UIUtils.getChartSeries(infos, window);
            ret.put("metrics", metrics);
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }



    @RequestMapping("/component/{component}")
    public Map components(@PathVariable String clusterName,
                          @PathVariable String topology,
                          @PathVariable String component,
                          @RequestParam(value = "window", required = false) String window){
        Map<String, Object> ret = new HashMap<>();
        int win = UIUtils.parseWindow(window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topology);
            MetricInfo componentMetrics = topologyInfo.get_metrics().get_componentMetric();

            ret.put("topologyId", topology);                                    //topology id
            ret.put("topologyName", topologyInfo.get_topology().get_name());    //topology name

            UIComponentMetric componentData = UIMetricUtils.getComponentMetric(componentMetrics, win,
                    component, topologyInfo.get_components());
            ret.put("componentMetrics", componentData);

            List<TaskEntity> taskEntities = UIUtils.getTaskEntities(topologyInfo, component);
            ret.put("taskStats", taskEntities);

            MetricInfo taskMetrics = client.getClient().getTaskMetrics(topology, component);
            List<UITaskMetric> taskData = UIMetricUtils.getTaskMetrics(taskMetrics, component, win);

            ret.put("taskMetrics", taskData);

        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }


    @RequestMapping("/task/{taskId}")
    public Map tasks(@PathVariable String clusterName, @PathVariable String topology,
                     @PathVariable Integer taskId,
                     @RequestParam(value = "window", required = false) String window){
        Map<String, Object> ret = new HashMap<>();
        int win = UIUtils.parseWindow(window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topology);
            String component = null;
            String type = null;
            for (ComponentSummary summary : topologyInfo.get_components()) {
                if (summary.get_taskIds().contains(taskId)){
                    component = summary.get_name();
                    type = summary.get_type();
                }
            }

            ret.put("topologyId", topology);
            ret.put("topologyName", topologyInfo.get_topology().get_name());
            ret.put("component", component);

            TaskEntity task = UIUtils.getTaskEntity(topologyInfo.get_tasks(), taskId);
            task.setComponent(component);
            task.setType(type);
            ret.put("task", task);

            List<MetricInfo> taskStreamMetrics = client.getClient().getTaskAndStreamMetrics(topology, taskId);
            UITaskMetric taskMetric = UIMetricUtils.getTaskMetric(taskStreamMetrics, component, taskId, win);
            ret.put("taskMetric", taskMetric);

            List<UIStreamMetric> streamMetrics = UIMetricUtils.getStreamMetrics(taskStreamMetrics, component, taskId, win);
            ret.put("streamMetrics", streamMetrics);
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            ret = UIUtils.exceptionJson(e);
            LOG.error(e.getMessage(), e);
        }
        return ret;
    }


}