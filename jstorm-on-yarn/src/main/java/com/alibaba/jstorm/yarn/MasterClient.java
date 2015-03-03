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
package com.alibaba.jstorm.yarn;

import java.util.Map;

import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.yarn.generated.StormMaster;
import com.alibaba.jstorm.yarn.generated.StormMaster.Client;
import com.alibaba.jstorm.yarn.thrift.ThriftClient;

import backtype.storm.utils.Utils;

public class MasterClient extends ThriftClient {
	private StormMaster.Client _client;
	private static final Logger LOG = LoggerFactory.getLogger(MasterClient.class);

	public MasterClient(Map storm_conf, String masterHost, int masterPort, Integer timeout) throws Exception {
		super(storm_conf, masterHost, masterPort, timeout);
        _client = new StormMaster.Client(_protocol);
	}

	public static MasterClient getConfiguredClient(Map conf) throws Exception {
		
		try {
            String masterHost = (String) conf.get(Config.MASTER_HOST);
//            String zookeeper = conf.get(Config.)
            LOG.info("masterHost is:" + masterHost);
            
            int masterPort = Utils.getInt(conf.get(Config.MASTER_THRIFT_PORT));
            LOG.info("masterPort is" + masterPort);
            System.out.println("masterPort is" + masterPort);
            try {
            	Integer timeout = Utils.getInt(conf.get(Config.MASTER_TIMEOUT_SECS));
            	return new MasterClient(conf, masterHost, masterPort, timeout);
            } catch (IllegalArgumentException e) {
            	return new MasterClient(conf, masterHost, masterPort, null);
            }
            
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
	}

	public Client getClient() {
		return _client;
	}

}
