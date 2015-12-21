package com.alibaba.jstorm.ui.controller;

import com.alibaba.jstorm.ui.model.ClusterConfig;
import com.alibaba.jstorm.ui.model.ZookeeperNode;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.ui.utils.ZookeeperManager;
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

    @RequestMapping(value = "/zookeeper/node", produces = "application/json;")
    @ResponseBody
    public Map<String, Object> getChildren(@RequestParam String path, String clusterName)  {
        List<ZookeeperNode> result  = ZookeeperManager.listZKNodes(clusterName, path);
        Map<String, Object> map = new HashMap<>();
        map.put("nodes", result);
        return map;
    }

    @RequestMapping(value = "/zookeeper/node/data", produces = "application/json;")
    @ResponseBody
    public Map<String, Object> getData(@RequestParam String path, String clusterName) {
        String data = ZookeeperManager.getZKNodeData(clusterName, path);
        Map<String, Object> map = new HashMap<>();
        map.put("data", data);
        return map;
    }
}

