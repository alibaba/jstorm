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
package com.alibaba.jstorm.zk;

public class ZkConstant {
	
	public static final String ZK_SEPERATOR = "/";

	public static final String ASSIGNMENTS_BAK = "assignments_bak";
	
	public static final String ASSIGNMENTS_BAK_SUBTREE;
	
	public static final String NIMBUS_SLAVE_ROOT = "nimbus_slave";
	
	public static final String NIMBUS_SLAVE_SUBTREE;
	
	static {
		ASSIGNMENTS_BAK_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_BAK;
		NIMBUS_SLAVE_SUBTREE = ZK_SEPERATOR + NIMBUS_SLAVE_ROOT;
	}
}
