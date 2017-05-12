package com.alibaba.jstorm.task.group;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.task.execute.MsgInfo;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.tuple.Fields;

/**
 * Two phase mappings: UserKey -> keyRange(VirtualNode) -> TargetTask(PhysicalNode)
 * @author basti.lj
 *
 */
public class MkKeyRangeFieldsGrouper extends MkFieldsGrouper {
    private static Logger LOG = LoggerFactory.getLogger(MkKeyRangeFieldsGrouper.class);
    private int keyRangeNum;
    private int targetTaskNum;

    public MkKeyRangeFieldsGrouper(Map conf, Fields outFields, Fields groupFields, List<Integer> outTasks) {
        super(outFields, groupFields, outTasks);
        this.targetTaskNum = outTasks.size();
        this.keyRangeNum = JStormUtils.getScaleOutNum(ConfigExtension.getKeyRangeNum(conf), targetTaskNum);
        LOG.info("MkKeyRangeFieldsGrouper: keyRangeNum={}, targetTaskNum={}", keyRangeNum, targetTaskNum);
    }

    public List<Integer> grouper(List<Object> values) {
        int hashCode = getHashCode(values);
        int group = hash(hashCode);
        return JStormUtils.mk_list(outTasks.get(group));
    }

    public void batchGrouper(List<MsgInfo> batch, Map<Object, List<MsgInfo>> ret) {
        for (MsgInfo msg : batch) {
            int hashCode = getHashCode(msg.values);
            int target = outTasks.get(hash(hashCode));
            List<MsgInfo> targetBatch = ret.get(target);
            if (targetBatch == null) {
                targetBatch = new ArrayList<>();
                ret.put(target, targetBatch);
            }
            targetBatch.add(msg);
        }
    }

    private int hash(int hashCode) {
        return Math.abs((hashCode % keyRangeNum) % targetTaskNum);
    }
}