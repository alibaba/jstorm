package com.alibaba.jstorm.task.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TMEvent implements Runnable{
	private static final Logger LOG = LoggerFactory.getLogger(TMEvent.class);
	
	TMHandler handler;
	Object input;
	
	public TMEvent(TMHandler handler, Object input) {
		this.handler = handler;
		this.input = input;
	}

	@Override
	public void run() {
		try {
			handler.process(input);
		}catch(Exception e) {
			LOG.error("Failed to handle input", e);
		}
	}

}
