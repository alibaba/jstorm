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
package backtype.storm.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class BatchTuple implements ITupleExt{
    private int targetTaskId;

    private List<Tuple> batch = new ArrayList<Tuple>();
    private int batchSize;

    public BatchTuple() {
        
    }
    
    public BatchTuple(int targetTaskId, int batchSize) {
        this.targetTaskId = targetTaskId;
        this.batchSize = batchSize;
    }

    public void addToBatch(Tuple tuple) {
        batch.add(tuple);
    }

    public boolean isBatchFull() {
        boolean ret = false;
        if (batch.size() >= batchSize)
            ret = true;

        return ret;
    }

    public List<Tuple> getTuples() { 
        return batch;
    }

    public void resetBatch() {
        batch = new ArrayList<Tuple>();
    }

    public int currBatchSize() {
        return batch.size();
    }

    public void setTargetTaskId(int taskId) {
        this.targetTaskId = taskId;
    }

    public int getTargetTaskId() {
        return targetTaskId;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

	@Deprecated
	public long getCreationTimeStamp() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Deprecated
	public void setCreationTimeStamp(long timeStamp) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}