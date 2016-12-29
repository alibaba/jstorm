/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.cluster;

import backtype.storm.LocalCluster;
import backtype.storm.LocalClusterMap;
import backtype.storm.LocalUtils;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.utils.TimeUtils;
import com.alibaba.jstorm.zk.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class StormZkClusterStateTest {
    LocalClusterMap state;
    StormZkClusterState stormClusterState;
    Factory zookeeper;
    @Before
    public void setUp() throws Exception {
        zookeeper = LocalUtils.startLocalZookeeper(LocalUtils.getTmpDir());
        Map conf = LocalUtils.getLocalConf(zookeeper.getZooKeeperServer().getClientPort());
        stormClusterState = (StormZkClusterState) Cluster.mk_storm_cluster_state(conf);
    }

    @After
    public void tearDown() throws Exception {
//        state.clean();
        zookeeper.closeAll();
    }

    @Test
    public void testReport_task_error() throws Exception {
        String topology_id = "topology_id_1";
        int task_id = 101;
        TaskError expected = new TaskError("task is dead", ErrorConstants.ERROR, ErrorConstants.CODE_TASK_DEAD, TimeUtils.current_time_secs());
        stormClusterState.report_task_error(topology_id, task_id, "task is dead", ErrorConstants.ERROR, ErrorConstants.CODE_TASK_DEAD);
        String path = Cluster.taskerror_path(topology_id, task_id);
        Map report_time = stormClusterState.topo_lastErr_time(topology_id);
        List<String> err_time = stormClusterState.task_error_time(topology_id, task_id);
        for (String time : err_time) {
            String errPath = path + Cluster.ZK_SEPERATOR + time;
            Object obj = stormClusterState.getObject(errPath, false);
            assertEquals(expected, obj);
        }
        stormClusterState.remove_task_error(topology_id, task_id);
        err_time = stormClusterState.task_error_time(topology_id, task_id);
        assertEquals(0, err_time.size());
    }
}