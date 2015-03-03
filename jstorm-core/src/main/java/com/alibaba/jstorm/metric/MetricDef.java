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
package com.alibaba.jstorm.metric;

public class MetricDef {
	// metric name for task
	public static final String DESERIALIZE_QUEUE = "Deserialize_Queue";
	public static final String DESERIALIZE_TIME  = "Deserialize_Time";
	public static final String SERIALIZE_QUEUE   = "Serialize_Queue";
	public static final String SERIALIZE_TIME    = "Serialize_Time";
	public static final String EXECUTE_QUEUE     = "Executor_Queue";
	public static final String EXECUTE_TIME      = "Execute_Time";
	public static final String ACKER_TIME        = "Acker_Time";
	public static final String EMPTY_CPU_RATIO   = "Empty_Cpu_Ratio";
	public static final String PENDING_MAP       = "Pending_Num";
	public static final String EMIT_TIME         = "Emit_Time";
	
	// metric name for worker
	public static final String NETWORK_MSG_TRANS_TIME = "Network_Transmit_Time";
	public static final String NETTY_SERV_DECODE_TIME = "Netty_Server_Decode_Time";
	public static final String DISPATCH_TIME          = "Virtual_Port_Dispatch_Time";
	public static final String DISPATCH_QUEUE         = "Virtual_Port_Dispatch_Queue";
	public static final String BATCH_TUPLE_TIME       = "Batch_Tuple_Time";
	public static final String BATCH_TUPLE_QUEUE      = "Batch_Tuple_Queue";
	public static final String DRAINER_TIME           = "Drainer_Time";
	public static final String DRAINER_QUEUE          = "Drainer_Queue";
	public static final String NETTY_CLI_SEND_TIME    = "Netty_Client_Send_Time";
	public static final String NETTY_CLI_BATCH_SIZE   = "Netty_Client_Send_Batch_Size";
	public static final String NETTY_CLI_SEND_PENDING = "Netty_Client_Send_Pendings";
	public static final String NETTY_CLI_SYNC_BATCH_QUEUE = "Netty_Client_Sync_BatchQueue";
	public static final String NETTY_CLI_SYNC_DISR_QUEUE  = "Netty_Client_Sync_DisrQueue";

    public static final String ZMQ_SEND_TIME     = "ZMQ_Send_Time";
    public static final String ZMQ_SEND_MSG_SIZE = "ZMQ_Send_MSG_Size";	

    public static final String CPU_USED_RATIO = "Used_Cpu";
    public static final String MEMORY_USED    = "Used_Memory";
    
	public static final String REMOTE_CLI_ADDR  = "Remote_Client_Address";
	public static final String REMOTE_SERV_ADDR = "Remote_Server_Address";
}
