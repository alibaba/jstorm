package backtype.storm.task;

import java.util.List;

public interface ICollectorCallback {
    public void execute(String stream, List<Integer> outTasks, List values);
}