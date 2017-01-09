package com.alibaba.jstorm.task.master.ctrlevent;

import java.io.Serializable;
import java.util.Map;

public class UpdateConfigEvent implements Serializable{
	private static final long serialVersionUID = -1505703098648481353L;

	private final Map conf;
	
	public UpdateConfigEvent(Map conf) {
		this.conf = conf;
	}

	public Map getConf() {
		return conf;
	}

}
