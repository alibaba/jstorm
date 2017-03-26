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

import java.util.HashSet;
import java.util.Set;

public class MetricDef {
    // metric name for task
    public static final String QUEUE_TYPE = "Queue";
    public static final String TIME_TYPE = "Time";

    public static final String DESERIALIZE_THREAD = "Deserialize";
    public static final String DESERIALIZE_QUEUE = DESERIALIZE_THREAD + QUEUE_TYPE;
    public static final String DESERIALIZE_TIME = DESERIALIZE_THREAD + TIME_TYPE;

    public static final String SERIALIZE_THREAD = "Serialize";
    public static final String SERIALIZE_QUEUE = SERIALIZE_THREAD + QUEUE_TYPE;
    public static final String SERIALIZE_TIME = SERIALIZE_THREAD + TIME_TYPE;

    public static final String EXECUTE_THREAD = "Executor";
    public static final String EXECUTE_QUEUE = EXECUTE_THREAD + QUEUE_TYPE;
    public static final String EXECUTE_TIME = EXECUTE_THREAD + TIME_TYPE;

    public static final String ACKER_TIME = "AckerTime";
    public static final String EMPTY_CPU_RATIO = "EmptyCpuRatio";
    public static final String PENDING_MAP = "PendingNum";
    public static final String COLLECTOR_EMIT_TIME = "EmitTime";
    public static final String TUPLE_LIEF_CYCLE = "TupleLifeCycle";
    public static final String CAPACITY = "Capacity";

    public static final String CONTROL_THREAD = "Ctrl";
    public static final String CONTROL_QUEUE = CONTROL_THREAD + QUEUE_TYPE;

    public static final String TASK_BATCH_INTERVAL_TIME = "BatchInterTime";

    public static final String RECV_CTRL_QUEUE = "RecvCtrl" + QUEUE_TYPE;

    public static final String RECV_THREAD = "RecvCtrl";
    public static final String BATCH_RECV_THREAD = "BatchRecvCtrl";
    public static final String BATCH_RECV_QUEUE = BATCH_RECV_THREAD + QUEUE_TYPE;

    public static final String SEND_THREAD = "SendCtrl";
    public static final String BATCH_SEND_THREAD = "BatchSendCtrl";
    public static final String BATCH_SEND_QUEUE = BATCH_SEND_THREAD + QUEUE_TYPE;

    public static final String SEND_QUEUE = "SendCtrl" + QUEUE_TYPE;

    public static final String NETWORK_MSG_DECODE_TIME = "MsgDecodeTime";

    // all tag start with "Netty" will be specially display in Web UI
    public static final String NETTY = "Netty";
    public static final String NETTY_CLI = NETTY + "Cli";
    public static final String NETTY_SRV = NETTY + "Srv";
    public static final String NETTY_CLI_SEND_SPEED = NETTY_CLI + "SendSpeed";
    public static final String NETTY_SRV_RECV_SPEED = NETTY_SRV + "RecvSpeed";

    public static final String NETTY_CLI_SEND_TIME = NETTY_CLI + "SendTime";
    public static final String NETTY_CLI_BATCH_SIZE = NETTY_CLI + "SendBatchSize";
    public static final String NETTY_CLI_SEND_PENDING = NETTY_CLI + "SendPending";
    public static final String NETTY_CLI_SYNC_BATCH_QUEUE = NETTY_CLI + "SyncBatchQ";
    public static final String NETTY_CLI_SYNC_DISR_QUEUE = NETTY_CLI + "SyncDisrQ";
    public static final String NETTY_CLI_CACHE_SIZE = NETTY_CLI + "CacheSize";
    public static final String NETTY_CLI_CONNECTION = NETTY_CLI + "ConnCheck";

    // metric name for worker
    public static final String NETTY_SRV_MSG_TRANS_TIME = NETTY_SRV + "TransmitTime";

    public static final String CPU_USED_RATIO = "CpuUsedRatio";
    public static final String MEMORY_USED = "MemoryUsed";
    public static final String HEAP_MEMORY_USED = "HeapMemory";
    public static final String MEMORY_USAGE = "MemoryUsage";
    public static final String DISK_USAGE = "DiskUsage";

    public static final String EMMITTED_NUM = "Emitted";
    public static final String ACKED_NUM = "Acked";
    public static final String FAILED_NUM = "Failed";
    public static final String SEND_TPS = "SendTps";
    public static final String RECV_TPS = "RecvTps";
    public static final String PROCESS_LATENCY = "ProcessLatency";
    public static final String FULL_GC = "FullGc";

    public static final String NETSENDSPEED = "NetSendSpeed";
    public static final String NETRECVSPEED = "NetRecvSpeed";

    public static final Set<String> MERGE_SUM_TAG = new HashSet<String>();

    static {
        MERGE_SUM_TAG.add(MetricDef.EMMITTED_NUM);
        MERGE_SUM_TAG.add(MetricDef.SEND_TPS);
        MERGE_SUM_TAG.add(MetricDef.RECV_TPS);
        MERGE_SUM_TAG.add(MetricDef.ACKED_NUM);
        MERGE_SUM_TAG.add(MetricDef.FAILED_NUM);

    }

    public static final Set<String> MERGE_AVG_TAG = new HashSet<String>();

    static {
        MERGE_AVG_TAG.add(PROCESS_LATENCY);
    }

    public static final double FULL_RATIO = 100.0;

    public static final String QEUEU_IS_FULL = "queue is full";

    public static final Set<String> TASK_QUEUE_SET = new HashSet<String>();

    static {
        TASK_QUEUE_SET.add(DESERIALIZE_QUEUE);
        TASK_QUEUE_SET.add(SERIALIZE_QUEUE);
        TASK_QUEUE_SET.add(EXECUTE_QUEUE);
        TASK_QUEUE_SET.add(CONTROL_QUEUE);

    }

    public static final Set<String> WORKER_QUEUE_SET = new HashSet<String>();

    static {
        WORKER_QUEUE_SET.add(RECV_CTRL_QUEUE);
        WORKER_QUEUE_SET.add(SEND_QUEUE);
    }

    public static final int NETTY_METRICS_PACKAGE_SIZE = 200;

    public static boolean isNettyDetails(String metricName) {

        Set<String> specialNettySet = new HashSet<String>();
        specialNettySet.add(MetricDef.NETTY_CLI_SEND_SPEED);
        specialNettySet.add(MetricDef.NETTY_SRV_RECV_SPEED);

        if (specialNettySet.contains(metricName)) {
            return false;
        }
        if (metricName.startsWith(MetricDef.NETTY)) {
            return true;
        }

        return false;
    }

}
