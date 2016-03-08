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
package com.alibaba.jstorm.ui.controller;

import java.util.List;
import java.util.Map;

import backtype.storm.generated.*;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.ui.model.*;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.utils.NimbusClient;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class ClusterController {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

    @RequestMapping(value = "/cluster", method = RequestMethod.GET)
    public String show(@RequestParam(value = "name", required = true) String name,
                       ModelMap model) {
        name = StringEscapeUtils.escapeHtml(name);
        long start = System.currentTimeMillis();
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(name);
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();

            model.addAttribute("nimbus", UIUtils.getNimbusEntities(clusterSummary));
            model.addAttribute("topologies", UIUtils.getTopologyEntities(clusterSummary));
            model.addAttribute("supervisors", UIUtils.getSupervisorEntities(clusterSummary));

            model.addAttribute("zkServers", UIUtils.getZooKeeperEntities(name));

            List<MetricInfo> clusterMetrics = client.getClient().getMetrics(JStormMetrics.CLUSTER_METRIC_KEY,
                    MetaType.TOPOLOGY.getT());
            UISummaryMetric clusterData = UIMetricUtils.getSummaryMetrics(clusterMetrics, AsmWindow.M1_WINDOW);
            model.addAttribute("clusterData", clusterData);
            model.addAttribute("clusterHead", UIMetricUtils.sortHead(clusterData, UISummaryMetric.HEAD));


            //update cluster cache
            ClusterEntity ce = UIUtils.getClusterEntity(clusterSummary, name);
            model.addAttribute("cluster", ce);
            UIUtils.clustersCache.put(name, ce);
        } catch (Exception e) {
            NimbusClientManager.removeClient(name);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }

        //set nimbus port and supervisor port , if necessary
        model.addAttribute("nimbusPort", UIUtils.getNimbusPort(name));
        model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(name));
        model.addAttribute("clusterName", name);
        model.addAttribute("page", "cluster");
        UIUtils.addTitleAttribute(model, "Cluster Summary");
        LOG.info("cluster page show cost:{}ms", System.currentTimeMillis() - start);
        return "cluster";
    }

}
