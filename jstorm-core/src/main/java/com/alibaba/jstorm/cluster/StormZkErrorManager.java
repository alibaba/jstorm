package com.alibaba.jstorm.cluster;

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by yunfan on 2017/2/28.
 */

public class StormZkErrorManager implements IStormErrorManager {

    private static Logger LOG = LoggerFactory.getLogger(StormZkErrorManager.class);

    private ClusterState cluster_state;

    @Override
    public void init(Map conf, ClusterState cluster_state) {
        this.cluster_state = cluster_state;
    }

    @Override
    public void report_task_error(String topologyId, int taskId, Throwable error) throws Exception {
        report_task_error(topologyId, taskId, JStormUtils.getErrorInfo(error),
                ErrorConstants.FATAL, ErrorConstants.CODE_USER);
    }

    @Override
    public void report_task_error(String topology_id, int task_id, String error) throws Exception {
        // we use this interface only in user level error
        report_task_error(topology_id, task_id, error, ErrorConstants.FATAL, ErrorConstants.CODE_USER);
    }

    @Override
    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code)
            throws Exception {
        report_task_error(topology_id, task_id, error, error_level, error_code, ErrorConstants.DURATION_SECS_DEFAULT);
    }

    @Override
    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code,
                                  int duration_secs) throws Exception {
        report_task_error(topology_id, task_id, error, error_level, error_code, duration_secs, null);
    }

    @Override
    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code,
                                  int duration_secs, String tag) throws Exception {
        boolean found = false;
        String path = Cluster.taskerror_path(topology_id, task_id);
        cluster_state.mkdirs(path);

        List<Integer> children = new ArrayList<Integer>();

        int timeSecs = TimeUtils.current_time_secs();
        String timestampPath = path + Cluster.ZK_SEPERATOR + timeSecs;
        TaskError taskError = new TaskError(error, error_level, error_code, timeSecs, duration_secs);

        for (String str : cluster_state.get_children(path, false)) {
            String errorPath = path + Cluster.ZK_SEPERATOR + str;
            Object obj =  getObject(errorPath, false);
            if (obj == null){
                deleteObject(errorPath);
                continue;
            }

            TaskError errorInfo = (TaskError) obj;

            // replace the old one if needed
            if (errorInfo.getError().equals(error)
                    || (tag != null && errorInfo.getError().startsWith(tag))) {
                cluster_state.delete_node(errorPath);
                setObject(timestampPath, taskError);
                found = true;
                break;
            }

            children.add(Integer.parseInt(str));
        }

        if (!found) {
            Collections.sort(children);

            while (children.size() >= 3) {
                deleteObject(path + Cluster.ZK_SEPERATOR + children.remove(0));
            }

            setObject(timestampPath, taskError);
        }
        setLastErrInfo(topology_id, duration_secs, timeSecs);
    }

    private static final String TASK_IS_DEAD = "is dead on"; // Full string is
    // "task-id is dead on hostname:port"

    private void setLastErrInfo(String topologyId, int duration, int timeStamp) throws Exception {
        // Set error information in task error topology patch
        // Last Error information format in ZK: map<report_duration, timestamp>
        // report_duration means only the errors will presented in web ui if the
        // error happens within this duration.
        // Currently, the duration for "queue full" error is 180sec(3min) while
        // the duration for other errors is 1800sec(30min).
        String lastErrTopoPath = Cluster.lasterror_path(topologyId);
        Map<Integer, String> lastErrInfo = null;
        try {
            lastErrInfo = (Map<Integer, String>) getObject(lastErrTopoPath, false);

        } catch (Exception e) {
            LOG.error("Failed to get last error time. Remove the corrupt node for " + topologyId, e);
            remove_lastErr_time(topologyId);
            lastErrInfo = null;
        }
        if (lastErrInfo == null)
            lastErrInfo = new HashMap<Integer, String>();

        // The error time is used to indicate how long the error info is present
        // in UI
        lastErrInfo.put(duration, timeStamp + "");
        setObject(lastErrTopoPath, lastErrInfo);
    }

    @Override
    public void remove_task_error(String topologyId, int taskId) throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        cluster_state.delete_node(path);
    }

    @Override
    public Map<Integer, String> topo_lastErr_time(String topologyId) throws Exception {
        String path = Cluster.lasterror_path(topologyId);

        return (Map<Integer, String>) getObject(path, false);
    }

    @Override
    public void remove_lastErr_time(String topologyId) throws Exception {
        String path = Cluster.lasterror_path(topologyId);
        deleteObject(path);
    }

    @Override
    public List<String> task_error_storms() throws Exception {
        return cluster_state.get_children(Cluster.TASKERRORS_SUBTREE, false);
    }

    @Override
    public List<String> task_error_ids(String topologyId) throws Exception {
        return cluster_state.get_children(Cluster.taskerror_storm_root(topologyId), false);
    }

    @Override
    public List<String> task_error_time(String topologyId, int taskId) throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        if (cluster_state.node_existed(path, false) == false) {
            return new ArrayList<String>();
        }
        return cluster_state.get_children(path, false);
    }

    public Object getObject(String path, boolean callback) throws Exception {
        byte[] data = cluster_state.get_data(path, callback);

        return Utils.maybe_deserialize(data);
    }

    public Object getObjectSync(String path, boolean callback) throws Exception {
        byte[] data = cluster_state.get_data_sync(path, callback);

        return Utils.maybe_deserialize(data);
    }

    public String getString(String path, boolean callback) throws Exception {
        byte[] data = cluster_state.get_data(path, callback);

        return new String(data);
    }

    public void deleteObject(String path) {
        try {
            cluster_state.delete_node(path);
        } catch (Exception e) {
            LOG.warn("Failed to delete node " + path);
        }
    }

    public void setObject(String path, Object obj) throws Exception {
        if (obj instanceof byte[]) {
            cluster_state.set_data(path, (byte[]) obj);
        } else if (obj instanceof String) {
            cluster_state.set_data(path, ((String) obj).getBytes());
        } else {
            cluster_state.set_data(path, Utils.serialize(obj));
        }
    }

    @Override
    public TaskError task_error_info(String topologyId, int taskId, long timeStamp) throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        path = path + "/" + timeStamp;
        return (TaskError) getObject(path, false);
    }

    @Override
    public List<TaskError> task_errors(String topologyId, int taskId) throws Exception {
        List<TaskError> errors = new ArrayList<TaskError>();
        String path = Cluster.taskerror_path(topologyId, taskId);
        if (cluster_state.node_existed(path, false) == false) {
            return errors;
        }

        List<String> children = cluster_state.get_children(path, false);


        for (String str : children) {
            Object obj = getObject(path + Cluster.ZK_SEPERATOR + str, false);
            if (obj != null) {
                TaskError error = (TaskError) obj;
                errors.add(error);
            }
        }

        Collections.sort(errors, new Comparator<TaskError>() {

            @Override
            public int compare(TaskError o1, TaskError o2) {
                if (o1.getTimSecs() > o2.getTimSecs()) {
                    return 1;
                }
                if (o1.getTimSecs() < o2.getTimSecs()) {
                    return -1;
                }
                return 0;
            }
        });

        return errors;

    }

    @Override
    public void teardown_task_errors(String topologyId) {
        try {
            String taskerrPath = Cluster.taskerror_storm_root(topologyId);
            deleteObject(taskerrPath);
        } catch (Exception e) {
            LOG.error("Could not teardown errors for " + topologyId, e);
        }
    }

}
