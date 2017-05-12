package backtype.storm.tuple;

import java.util.Iterator;
import java.util.List;

public interface ITupleExt {

    int getTargetTaskId();

    void setTargetTaskId(int targetTaskId);

    /**
     * Get creating timestamp of a tuple
     */
    long getCreationTimeStamp();

    void setCreationTimeStamp(long timeStamp);

    boolean isBatchTuple();

    void setBatchTuple(boolean isBatchTuple);

    long getBatchId();

    void setBatchId(long batchId);

    public Iterator<List<Object>> valueIterator();
}
