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
package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ClusterInfo;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.zk.ZkTool;
import com.google.common.collect.Lists;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "clusterpage")
@ViewScoped
public class ClusterPage implements Serializable {

	private static final long serialVersionUID = -6103468603521876731L;

	private static final Logger LOG = Logger.getLogger(ClusterPage.class);

	public static String SINGLE_CLUSTER = "single";
	public static String MULTI_CLUSTER = "multi";
	
	private List<Map> uiClusters = null;
	private List<ClusterInfo> clusterInfos = null;
	private String clusterType;

	public ClusterPage() throws Exception {
		init();
	}

	@SuppressWarnings("rawtypes")
	private void init() throws Exception {

		try {
			LOG.info("ClusterPage init...");
			Map conf = UIUtils.readUiConfig();
			uiClusters = ConfigExtension.getUiClusters(conf);
			
			if (uiClusters != null) {
				clusterType = MULTI_CLUSTER;
			    clusterInfos = new ArrayList<ClusterInfo>();	
			    for (Map cluster : uiClusters) {
				    LOG.debug("Get ui cluster config infor, " + cluster);
				    ClusterInfo clusterInfo = new ClusterInfo();
				    clusterInfo.setClusterName(ConfigExtension.getUiClusterName(cluster));
				    clusterInfo.setZkPort(ConfigExtension.getUiClusterZkPort(cluster));
				    clusterInfo.setZkRoot(ConfigExtension.getUiClusterZkRoot(cluster));
				    clusterInfo.setZkServers(ConfigExtension.getUiClusterZkServers(cluster));
				    clusterInfos.add(clusterInfo);
			    }
			} else {
				clusterType = SINGLE_CLUSTER;
			}

		} catch (Exception e) {
			LOG.error("Failed to get cluster information:", e);
			throw e;
		} finally {
		}
	}

	public List<ClusterInfo> getClusterInfo() {
		return clusterInfos;
	}
	
	public String getClusterType() {
		return clusterType;
	}
	
	public static void main(String[] args) {
		try {
			ClusterPage c = new ClusterPage();
			System.out.println(c.getClusterInfo());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
