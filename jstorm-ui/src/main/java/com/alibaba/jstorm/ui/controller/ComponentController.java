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

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.ui.model.UIComponentMetric;
import com.alibaba.jstorm.ui.model.TaskEntity;
import com.alibaba.jstorm.ui.model.UITaskMetric;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class ComponentController {
    private static final Logger LOG = LoggerFactory.getLogger(ComponentController.class);

    @RequestMapping(value = "/component", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "topology", required = true) String topology_id,
                       @RequestParam(value = "component", required = true) String component,
                       @RequestParam(value = "win", required = false) String win,
                       ModelMap model) {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        topology_id = StringEscapeUtils.escapeHtml(topology_id);
        long start = System.currentTimeMillis();
        int window = UIUtils.parseWindow(win);
        UIUtils.addWindowAttribute(model, window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            //get Component Metric
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topology_id);
            MetricInfo componentMetrics = topologyInfo.get_metrics().get_componentMetric();
//            List<MetricInfo> componentMetrics = client.getClient().getMetrics(topology_id, MetaType.COMPONENT.getT());
            UIComponentMetric componentMetric = UIMetricUtils.getComponentMetric(componentMetrics, window,
                    component, topologyInfo.get_components());
            model.addAttribute("comp", componentMetric);
            model.addAttribute("compHead", UIMetricUtils.sortHead(componentMetric, UIComponentMetric.HEAD));

            //get Task Stat
            model.addAttribute("tasks", UIUtils.getTaskEntities(topologyInfo, component));

            //get Task metrics
//            MetricInfo taskMetrics = topologyInfo.get_metrics().get_taskMetric();
            MetricInfo taskMetrics = client.getClient().getTaskMetrics(topology_id, component);
//            System.out.println("taskMetrics:" + taskMetrics);
            List<UITaskMetric> taskData = UIMetricUtils.getTaskMetrics(taskMetrics, component, window);
            model.addAttribute("taskData", taskData);
            model.addAttribute("taskHead", UIMetricUtils.sortHead(taskData, UITaskMetric.HEAD));

        } catch (NotAliveException nae) {
            model.addAttribute("flush", String.format("The topology: %s is dead.", topology_id));
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }

        // page information
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("topologyId", topology_id);
        model.addAttribute("compName", component);
        model.addAttribute("page", "component");
        model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(clusterName));
        UIUtils.addTitleAttribute(model, "Component Summary");
        try {
            String topologyName = Common.topologyIdToName(topology_id);
            model.addAttribute("topologyName", topologyName);
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }

        LOG.info("component page show cost:{}ms", System.currentTimeMillis() - start);
        return "component";
    }



}
