package com.alibaba.jstorm.cluster;

import com.alibaba.jstorm.task.error.TaskError;

import java.util.List;
import java.util.Map;

/**
 * Created by yunfan on 2017/2/28.
 */
public interface IStormErrorManager {

    public void init(Map conf, ClusterState obj);

    public List<String> task_error_storms() throws Exception;

    public List<String> task_error_ids(String topologyId) throws Exception;

    public void report_task_error(String topology_id, int task_id, Throwable error) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code, int duration) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code, int duration, String tag) throws Exception;

    public Map<Integer, String> topo_lastErr_time(String topologyId) throws Exception;

    public void remove_lastErr_time(String topologyId) throws Exception;

    public List<TaskError> task_errors(String topology_id, int task_id) throws Exception;

    public void remove_task_error(String topologyId, int taskId) throws Exception;

    public List<String> task_error_time(String topologyId, int taskId) throws Exception;

    public TaskError task_error_info(String topologyId, int taskId, long timeStamp) throws Exception;

    public void teardown_task_errors(String topology_id) throws Exception;
}
