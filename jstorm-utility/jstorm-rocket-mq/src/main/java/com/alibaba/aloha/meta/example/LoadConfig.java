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
package com.alibaba.aloha.meta.example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.Yaml;

public class LoadConfig {
	public static final String TOPOLOGY_TYPE = "topology.type";
	
	private static Map LoadProperty(String prop) {
		Map ret = null;
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
			ret = new HashMap<Object, Object>();
			ret.putAll(properties);
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		return ret;
	}

	private static Map LoadYaml(String confPath) {
        Map ret = null;
		Yaml yaml = new Yaml();

		try {
			InputStream stream = new FileInputStream(confPath);

			ret = (Map) yaml.load(stream);
			if (ret == null || ret.isEmpty() == true) {
				throw new RuntimeException("Failed to read config file");
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
	
	public static Map LoadConf(String arg) {
		Map ret = null;
		
		if (arg.endsWith("yaml")) {
			ret = LoadYaml(arg);
		} else {
			ret = LoadProperty(arg);
		}
		
		return ret;
	}
}