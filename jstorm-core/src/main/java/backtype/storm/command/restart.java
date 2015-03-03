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
package backtype.storm.command;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Active topology
 * 
 * @author basti
 * 
 */
public class restart {	
	private static Map LoadProperty(String prop) {
		Map ret = new HashMap<Object, Object>();
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
			if (properties.size() == 0) {
				System.out.println("WARN: Config file is empty");
				return null;
			} else {
				ret.putAll(properties);
			}
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
			throw new RuntimeException(e.getMessage());
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1.getMessage());
		}
		
		return ret;
	}

	private static Map LoadYaml(String confPath) {
		Map ret = new HashMap<Object, Object>();
		Yaml yaml = new Yaml();

		try {
			InputStream stream = new FileInputStream(confPath);
			ret = (Map) yaml.load(stream);
			if (ret == null || ret.isEmpty() == true) {
				System.out.println("WARN: Config file is empty");
				return null;
			}
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + confPath);
			throw new RuntimeException("No config file");
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new RuntimeException("Failed to read config file");
		}

		return ret;
	}
	
	private static Map LoadConf(String arg) {
		Map ret = null;
		if (arg.endsWith("yaml")) {
			ret = LoadYaml(arg);
		} else {
			ret = LoadProperty(arg);
		}
		return ret;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args == null || args.length == 0) {
			throw new InvalidParameterException("Should input topology name");
		}

		String topologyName = args[0];

		NimbusClient client = null;
		try {
			Map conf = Utils.readStormConfig();
			client = NimbusClient.getConfiguredClient(conf);
			
			System.out.println("It will take 15 ~ 100 seconds to restart, please wait patiently\n");

			if (args.length == 1) {
				client.getClient().restart(topologyName, null);
			} else {
				Map loadConf = LoadConf(args[1]);
				String jsonConf = Utils.to_json(loadConf);
				System.out.println("New configuration:\n" + jsonConf);
				
				client.getClient().restart(topologyName, jsonConf);
			}

			System.out.println("Successfully submit command restart "
					+ topologyName);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

}
