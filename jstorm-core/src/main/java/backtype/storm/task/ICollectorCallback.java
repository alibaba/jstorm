package backtype.storm.task;

import java.util.List;

public interface ICollectorCallback {
    void execute(String stream, List<Integer> outTasks, List values);
}