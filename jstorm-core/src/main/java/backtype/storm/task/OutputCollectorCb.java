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
package backtype.storm.task;

import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public abstract class OutputCollectorCb implements IOutputCollector {
    protected IOutputCollector delegate;

    public OutputCollectorCb() {
    }

    public OutputCollectorCb(IOutputCollector delegate) {
        this.delegate = delegate;
    }

    public abstract List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback);

    public abstract void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback);

    public abstract List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple);

    public abstract void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple);

    public void flush() {

    }

    public void setBatchId(long batchId) {

    }
}
