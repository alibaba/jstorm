/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.ui.controller;

import com.alibaba.jstorm.ui.model.ClusterConfig;
import com.alibaba.jstorm.ui.model.ZookeeperNode;
import com.alibaba.jstorm.ui.utils.UIDef;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.ui.utils.ZookeeperManager;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Donne (lindeqiang1988@gmail.com)
 */
@Controller
public class ZookeeperController {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperController.class);


    @RequestMapping(value = "/zookeeper", method = RequestMethod.GET)
    public String show(@RequestParam(value = "name", required = true) String name,
                       ModelMap model) {
        name = StringEscapeUtils.escapeHtml(name);
        long start = System.currentTimeMillis();
        try{
            ClusterConfig config = UIUtils.clusterConfig.get(name);
            StringBuilder builder = new StringBuilder("");
            for (String ip:config.getZkServers()){
                builder.append(ip).append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(":");
            builder.append(config.getZkPort());
            model.addAttribute("zkRoot", config.getZkRoot());
            model.addAttribute("zkServers", builder.toString());
            model.addAttribute("clusterName",name);
        }catch (Exception e){
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }

        LOG.info("zookeeper page show cost:{}ms", System.currentTimeMillis() - start);
        return "zookeeper";
    }

    @RequestMapping(value = UIDef.API_V2 + "/zookeeper/node", produces = "application/json;")
    @ResponseBody
    public Map<String, Object> getChildren(@RequestParam String path, String clusterName)  {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        List<ZookeeperNode> result  = ZookeeperManager.listZKNodes(clusterName, path);
        Map<String, Object> map = new HashMap<>();
        map.put("nodes", result);
        return map;
    }

    @RequestMapping(value = UIDef.API_V2 + "/zookeeper/node/data", produces = "application/json;")
    @ResponseBody
    public Map<String, Object> getData(@RequestParam String path, String clusterName) {
        clusterName = StringEscapeUtils.escapeHtml(clusterName);
        String data = ZookeeperManager.getZKNodeData(clusterName, path);
        Map<String, Object> map = new HashMap<>();
        map.put("data", data);
        return map;
    }
}

