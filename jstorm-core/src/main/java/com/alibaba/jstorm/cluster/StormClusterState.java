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
package com.alibaba.jstorm.cluster;

import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.nimbus.NimbusInfo;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.AssignmentBak;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * all storm in zk operation interface
 */
public interface StormClusterState {
    void disconnect() throws Exception;

    void remove_storm(String topology_id) throws Exception;

    void try_remove_storm(String topology_id);

    List<String> assignments(RunnableCallback callback) throws Exception;

    Assignment assignment_info(String topology_id, RunnableCallback callback) throws Exception;

    Integer assignment_version(String topology_id, RunnableCallback callback) throws Exception;

    void set_assignment(String topology_id, Assignment info) throws Exception;

    AssignmentBak assignment_bak(String topologyName) throws Exception;

    void backup_assignment(String topology_id, AssignmentBak info) throws Exception;

    List<String> active_storms() throws Exception;

    StormBase storm_base(String topology_id, RunnableCallback callback) throws Exception;

    void activate_storm(String topology_id, StormBase storm_base) throws Exception;

    void update_storm(String topology_id, StormStatus new_elems) throws Exception;

    void set_storm_monitor(String topologyId, boolean isEnable) throws Exception;

    void remove_storm_base(String topology_id) throws Exception;

    List<String> task_storms() throws Exception;

    Set<Integer> task_ids(String topology_id) throws Exception;

    Set<Integer> task_ids_by_componentId(String topologyId, String componentId) throws Exception;

    void set_task(String topologyId, Map<Integer, TaskInfo> taskInfoMap) throws Exception;

    void add_task(String topology_id, Map<Integer, TaskInfo> taskInfoMap) throws Exception;

    void remove_task(String topologyId, Set<Integer> taskIds) throws Exception;

    Map<Integer, TaskInfo> task_all_info(String topology_id) throws Exception;

    List<String> heartbeat_storms() throws Exception;

    void topology_heartbeat(String topology_id, TopologyTaskHbInfo info) throws Exception;

    TopologyTaskHbInfo topology_heartbeat(String topologyId) throws Exception;

    void teardown_heartbeats(String topology_id) throws Exception;

    List<String> task_error_storms() throws Exception;

    List<String> task_error_ids(String topologyId) throws Exception;

    void report_task_error(String topology_id, int task_id, Throwable error) throws Exception;

    void report_task_error(String topology_id, int task_id, String error) throws Exception;

    void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code) throws Exception;

    void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code, int duration) throws Exception;

    void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code, int duration, String tag) throws Exception;

    Map<Integer, String> topo_lastErr_time(String topologyId) throws Exception;

    void remove_lastErr_time(String topologyId) throws Exception;

    List<TaskError> task_errors(String topology_id, int task_id) throws Exception;

    void remove_task_error(String topologyId, int taskId) throws Exception;

    List<String> task_error_time(String topologyId, int taskId) throws Exception;

    TaskError task_error_info(String topologyId, int taskId, long timeStamp) throws Exception;

    void teardown_task_errors(String topology_id) throws Exception;

    List<String> supervisors(RunnableCallback callback) throws Exception;

    SupervisorInfo supervisor_info(String supervisor_id) throws Exception;

    void supervisor_heartbeat(String supervisor_id, SupervisorInfo info) throws Exception;

    boolean try_to_be_leader(String path, String host, RunnableCallback callback) throws Exception;

    String get_leader_host() throws Exception;

    boolean leader_existed() throws Exception;

    List<String> get_nimbus_slaves() throws Exception;

    void update_nimbus_slave(String host, int time) throws Exception;

    String get_nimbus_slave_time(String host) throws Exception;

    void unregister_nimbus_host(String host) throws Exception;

    void update_nimbus_detail(String hostPort, Map map) throws Exception;

    Map get_nimbus_detail(String hostPort, boolean watch) throws Exception;

    void unregister_nimbus_detail(String hostPort) throws Exception;

    void set_topology_metric(String topologyId, Object metric) throws Exception;

    Object get_topology_metric(String topologyId) throws Exception;

    List<String> get_metrics() throws Exception;

    List<String> list_dirs(String path, boolean watch) throws Exception;

    // sets up information related to key consisting of nimbus
    // host:port and version info of the blob
    void setup_blobstore(String key, NimbusInfo nimbusInfo, int versionInfo) throws Exception;

    List<String> active_keys() throws Exception;

    List<String> blobstore(RunnableCallback callback) throws Exception;

    List<String> blobstoreInfo(String blobKey) throws Exception;

    /**
     * Deletes the state inside the zookeeper for a key,
     * for which the contents of the key starts with nimbus host port information
     */
    void delete_node_blobstore(String parentPath, String hostPortInfo) throws Exception;

    void remove_blobstore_key(String blobKey) throws Exception;

    void remove_key_version(String blobKey) throws Exception;

    void mkdir(String path);

    void set_in_blacklist(String host) throws Exception;

    void remove_from_blacklist(String host) throws Exception;

    List<String> get_blacklist() throws Exception;

    List<String> get_upgrading_topologies() throws Exception;

    Object get_gray_upgrade_conf(String topologyId) throws Exception;

    List<String> get_upgrading_workers(String topologyId) throws Exception;

    List<String> get_upgraded_workers(String topoogyId) throws Exception;

    void add_upgrading_worker(String topologyId, String hostPort) throws Exception;

    void remove_upgrading_worker(String topologyId, String hostPort) throws Exception;

    void add_upgraded_worker(String topologyId, String hostPort) throws Exception;

    void set_gray_upgrade_conf(String topologyId, Object obj) throws Exception;

    void remove_gray_upgrade_info(String topologyId) throws Exception;
}
