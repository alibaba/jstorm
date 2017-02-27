/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.yarn.constants;

import org.apache.hadoop.registry.client.api.RegistryConstants;

/**
 * These are the keys that can be added to <code>conf/jstorm-yarn-client.xml</code>.
 */
public interface JstormXmlConfKeys {

  /**
   *
   * Use {@link RegistryConstants#KEY_REGISTRY_ZK_ROOT}
   *
   */
  @Deprecated
  String REGISTRY_PATH = "jstorm.registry.path";

  /**
   * 
   * @Deprecated use {@link RegistryConstants#KEY_REGISTRY_ZK_QUORUM}
   * 
   */
  @Deprecated
  String REGISTRY_ZK_QUORUM = "jstorm.zookeeper.quorum";


  String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH =
      "ipc.client.fallback-to-simple-auth-allowed";
}
