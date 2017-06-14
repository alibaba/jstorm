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
package com.alibaba.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.ons.consumer.ConsumerSpout;
import com.alibaba.jstorm.ons.producer.ProducerBolt;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.HashMap;
import java.util.Map;

public class TestTopology {

	private static Map conf = new HashMap<Object, Object>();

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Please input configuration file");
			System.exit(-1);
		}

		conf = LoadConfig.LoadConf(args[0]);

		TopologyBuilder builder = setupBuilder();

		submitTopology(builder);

	}

	private static TopologyBuilder setupBuilder() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		int writerParallel = JStormUtils.parseInt(conf.get("topology.producer.parallel"), 1);

		int spoutParallel = JStormUtils.parseInt(conf.get("topology.consumer.parallel"), 1);

		builder.setSpout("OnsConsumer", new ConsumerSpout(), spoutParallel);

		builder.setBolt("OnsProducer", new ProducerBolt(), writerParallel).localFirstGrouping("OnsConsumer");

		return builder;
	}

	private static void submitTopology(TopologyBuilder builder) {
		try {
			if (local_mode(conf)) {

				LocalCluster cluster = new LocalCluster();

				cluster.submitTopology(String.valueOf(conf.get("topology.name")), conf, builder.createTopology());

				Thread.sleep(200000);

				cluster.shutdown();
			} else {
				StormSubmitter.submitTopology(String.valueOf(conf.get("topology.name")), conf,
						builder.createTopology());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean local_mode(Map conf) {
		String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
		if (mode != null) {
			if (mode.equals("local")) {
				return true;
			}
		}

		return false;
	}

}
