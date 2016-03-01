package backtype.storm.task;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface ICollectorCallback {
    public void execute(List<Integer> outTasks);
}