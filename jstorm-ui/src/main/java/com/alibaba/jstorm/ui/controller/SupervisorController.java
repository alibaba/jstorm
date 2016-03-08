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

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.ui.model.SupervisorEntity;
import com.alibaba.jstorm.ui.model.UIWorkerMetric;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class SupervisorController {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorController.class);

    @RequestMapping(value = "/supervisor", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "host", required = true) String host,
                       @RequestParam(value = "win", required = false) String win,
                       ModelMap model) {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        long start = System.currentTimeMillis();
        host = NetWorkUtils.host2Ip(host);
        int window = UIUtils.parseWindow(win);
        UIUtils.addWindowAttribute(model, window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            //get supervisor summary
            SupervisorWorkers supervisorWorkers = client.getClient().getSupervisorWorkers(host);
            model.addAttribute("supervisor", new SupervisorEntity(supervisorWorkers.get_supervisor()));

            //get worker summary
            List<WorkerSummary> workerSummaries = supervisorWorkers.get_workers();
            model.addAttribute("workerSummary", UIUtils.getWorkerEntities(workerSummaries));

            //get worker metrics
            Map<String, MetricInfo> workerMetricInfo = supervisorWorkers.get_workerMetric();
            List<UIWorkerMetric> workerMetrics = UIMetricUtils.getWorkerMetrics(workerMetricInfo,
                    workerSummaries, host, window);
//            System.out.println("workerMetricInfo:"+workerMetricInfo);
            model.addAttribute("workerMetrics", workerMetrics);
            model.addAttribute("workerHead", UIMetricUtils.sortHead(workerMetrics, UIWorkerMetric.HEAD));


        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }
        // page information
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("host", host);
        model.addAttribute("page", "supervisor");
        model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(clusterName));
        UIUtils.addTitleAttribute(model, "Supervisor Summary");

        LOG.info("supervisor page show cost:{}ms", System.currentTimeMillis() - start);
        return "supervisor";
    }




}
