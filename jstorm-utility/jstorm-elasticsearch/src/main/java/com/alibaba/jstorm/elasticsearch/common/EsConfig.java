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
package com.alibaba.jstorm.elasticsearch.common;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class EsConfig implements Serializable {
  
  private static final long serialVersionUID = 946576929049344226L;
  
  private static final String SEPARATER = ",";
  private static final String DELIMITER = ":";

  private final String clusterName;
  private final String[] nodes;
  private final Map<String, String> additionalConfiguration;

  public EsConfig(String clusterName, String nodes) {
    this(clusterName, nodes, Collections.<String, String> emptyMap());
  }

  public EsConfig(String clusterName, String nodes,
      Map<String, String> additionalConfiguration) {
    this.clusterName = clusterName;
    this.nodes = splitNodes(nodes);
    this.additionalConfiguration = new HashMap<String, String>(
        additionalConfiguration);
    checkArguments();
  }

  private String[] splitNodes(String nodesString) {
    String[] nodes = nodesString.split(SEPARATER);
    return nodes;
  }

  private void checkArguments() {
    Preconditions.checkNotNull(clusterName);
    Preconditions.checkNotNull(additionalConfiguration);
    Preconditions.checkNotNull(nodes);
    Preconditions.checkArgument(nodes.length != 0, "Nodes cannot be empty");
  }

  List<TransportAddress> getTransportAddresses() throws UnknownHostException {
    List<TransportAddress> transportAddresses = Lists.newArrayList();
    for (String node : nodes) {
      String[] hostAndPort = node.split(DELIMITER);
      Preconditions.checkArgument(hostAndPort.length == 2,
          "Incorrect node format");
      String host = hostAndPort[0];
      int port = Integer.parseInt(hostAndPort[1]);
      InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(
          InetAddress.getByName(host), port);
      transportAddresses.add(inetSocketTransportAddress);
    }
    return transportAddresses;
  }

  Settings getSettings() {
    return Settings.builder().put("cluster.name", clusterName)
        .put(additionalConfiguration).build();
  }

}
