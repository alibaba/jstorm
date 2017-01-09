package com.alibaba.aloha.meta.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import com.alibaba.aloha.meta.MetaSpout;
import com.alibaba.aloha.meta.MetaTuple;

public class TestTridentTopology {
	private static final Logger LOG = Logger.getLogger(TestTridentTopology.class);
	
	public static class MsgPrint extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			MetaTuple metaTuple = (MetaTuple) tuple.getValue(0);
			LOG.info("Messages: " + metaTuple);
			collector.emit(null);
		}
	}
	
	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		topology.newStream("MetaSpout", new MetaSpout()).each(new Fields("MetaTuple"), new MsgPrint(), new Fields());
		
		return topology.build();
	}
	
	public static void main(String[] args) throws Exception{
		if (args.length == 0) {
			System.err.println("Please input configuration file");
			System.exit(-1);
		}

		Map conf = LoadConfig.LoadConf(args[0]);	

		if (conf == null) {
			LOG.error("Failed to load config");
		} else {
			Config config = new Config();
			config.putAll(conf);
	        config.setMaxSpoutPending(10);
	        config.put(LoadConfig.TOPOLOGY_TYPE, "Trident");
	        StormSubmitter.submitTopology("WordCount", config, buildTopology());
		}
	}
}