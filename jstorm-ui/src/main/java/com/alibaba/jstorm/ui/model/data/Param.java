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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import com.alibaba.jstorm.common.stats.StatBuckets;

/**
 * 
 * @author xin.zhou
 */
@ManagedBean(name = "pm")
@ViewScoped
public class Param implements Serializable {

	private static final long serialVersionUID = -1087749257427646824L;
	private String clusterName = null;
	private String topologyid = "";
	private String window = null;
	private String componentid = "";
	private String taskid = "";

	public Param() {
		init();
	}

	private void init() {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("topologyid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("window") != null) {
			window = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("window");
		}
		if (ctx.getExternalContext().getRequestParameterMap()
				.get("componentid") != null) {
			componentid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("componentid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("taskid") != null) {
			taskid = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("taskid");
		}

		window = StatBuckets.getShowTimeStr(window);
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getTopologyid() {
		return topologyid;
	}

	public void setTopologyid(String topologyid) {
		this.topologyid = topologyid;
	}

	public String getWindow() {
		return window;
	}

	public void setWindow(String window) {
		this.window = window;
	}

	public String getComponentid() {
		return componentid;
	}

	public void setComponentid(String componentid) {
		this.componentid = componentid;
	}

	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}
}
