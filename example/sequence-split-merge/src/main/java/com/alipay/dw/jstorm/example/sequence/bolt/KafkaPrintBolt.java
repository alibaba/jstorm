package com.alipay.dw.jstorm.example.sequence.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class KafkaPrintBolt implements IBasicBolt {
	public static Logger LOG = LoggerFactory.getLogger(KafkaPrintBolt.class);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		LOG.info("------------------------------------------tuple = " + tuple.toString() + " ------------------------------------------ " ) ;
		LOG.info("------------------------------------------tuple.getSourceComponent = " + tuple.getSourceComponent() + " ------------------------------------------ " ) ;
		LOG.info("------------------------------------------tuple.getSourceStreamId = " + tuple.getSourceStreamId() + " ------------------------------------------ " ) ;
		
		String message;
		try {
			message = new String(tuple.getBinaryByField("bytes"), "UTF-8");
			LOG.info("Message:{}", message);
			LOG.info("------------------------------------------" + tuple.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			
			LOG.info("------------------------------------------tuple error ------------------------------" );
			
			e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}