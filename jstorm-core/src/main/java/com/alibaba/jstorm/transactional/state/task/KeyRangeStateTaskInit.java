package com.alibaba.jstorm.transactional.state.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.transactional.state.KeyRangeState;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.utils.JStormUtils;

public class KeyRangeStateTaskInit implements ITaskStateInitOperator {
    private static final Logger LOG = getLogger(KeyRangeStateTaskInit.class);

    @Override
    public Object getTaskInitState(Map conf, int currTaskId, Set<Integer> currComponentTasks, Map<Integer, TransactionState> prevComponentTaskStates) {
        Map<Integer, String> currKeyRangeToCheckpoints = new HashMap<Integer, String>();
        Map<Integer, String> prevKeyRangeToCheckpoints = getAllKeyRangeCheckpointPaths(prevComponentTaskStates.values());
        int componentTaskNum = currComponentTasks.size();
        int taskIndex = JStormUtils.getTaskIndex(currTaskId, currComponentTasks);
        int prevKeyRangeNum = prevKeyRangeToCheckpoints.size();
        int configKeyRangeNum = ConfigExtension.getKeyRangeNum(conf);
        int currKeyRangeNum = JStormUtils.getScaleOutNum(configKeyRangeNum, componentTaskNum);
        Collection<Integer> currKeyRanges = KeyRangeState.keyRangesByTaskIndex(currKeyRangeNum, componentTaskNum, taskIndex);
        LOG.debug("currKeyRanges: {}, prevKeyRangeToCheckpoints : {}", currKeyRanges, prevKeyRangeToCheckpoints);

        // Check if scale-out of key range happens
        if (currKeyRangeNum > prevKeyRangeNum) {
            // key range number increases by 2^n times by default
            for (Integer keyRange : currKeyRanges) {
                currKeyRangeToCheckpoints.put(keyRange, prevKeyRangeToCheckpoints.get(keyRange % prevKeyRangeNum));
            }
        } else {
            for (Integer keyRange : currKeyRanges) {
                currKeyRangeToCheckpoints.put(keyRange, prevKeyRangeToCheckpoints.get(keyRange));
            }
        }
        return currKeyRangeToCheckpoints;
    }

    private Map<Integer, String> getAllKeyRangeCheckpointPaths(Collection<TransactionState> allStates) {
        Map<Integer, String> keyRangeToCheckpoints = new HashMap<Integer, String>();
        for (TransactionState state : allStates) {
            Map<Integer, String> checkpoint = (Map<Integer, String>) state.getUserCheckpoint();
            if (checkpoint != null)
                keyRangeToCheckpoints.putAll(checkpoint);
        }
        return keyRangeToCheckpoints;
        
    }
}