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

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import backtype.storm.task.GeneralTopologyContext;

public class TupleImplExt extends TupleImpl implements TupleExt {

    protected int targetTaskId;
    protected long creationTimeStamp;
    protected boolean isBatchTuple = false;

    public TupleImplExt() {
        
    }

    public TupleImplExt(GeneralTopologyContext context, List<Object> values, int taskId, String streamId) {
        this(context, values, taskId, streamId, MessageId.makeUnanchored());
    }

    public TupleImplExt(GeneralTopologyContext context, List<Object> values, int taskId, String streamId, MessageId id) {
        super(context, values, taskId, streamId, id);
        creationTimeStamp = System.currentTimeMillis();
    }
    
    public TupleImplExt(GeneralTopologyContext context, List<Object> values, MessageId id, TupleImplExt tuple) {
    	super(context, values, tuple.getSourceTask(), tuple.getSourceStreamId(), id);
    	this.targetTaskId = tuple.getTargetTaskId();
    	this.creationTimeStamp = tuple.getCreationTimeStamp();
    }

    @Override
    public int getTargetTaskId() {
        return targetTaskId;
    }

    @Override
    public void setTargetTaskId(int targetTaskId) {
        this.targetTaskId = targetTaskId;
    }

	@Override
	public long getCreationTimeStamp() {
		return creationTimeStamp;
	}

	@Override
	public void setCreationTimeStamp(long timeStamp) {
		this.creationTimeStamp = timeStamp;
	}

	@Override
	public boolean isBatchTuple() {
		return isBatchTuple;
	}

	@Override
	public void setBatchTuple(boolean isBatchTuple) {
		this.isBatchTuple = isBatchTuple;
	}

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
