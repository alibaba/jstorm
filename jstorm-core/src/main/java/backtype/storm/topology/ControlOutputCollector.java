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
package backtype.storm.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.task.execute.BoltCollector;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * callbacks like ICollectorCallback will be unnecessary since control messages are sent sequentially
 *
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ControlOutputCollector implements IControlOutputCollector {

    private OutputCollector out;

    public ControlOutputCollector(OutputCollector out) {
        this.out = out;
    }

    public List<Integer> emit(String streamId, Collection<Tuple> anchors,
                              List<Object> tuple) {
        return out.emit(streamId, anchors, tuple);
    }

    public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple) {
        return out.emit(streamId, Arrays.asList(anchor), tuple);
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return out.emit(streamId, (List) null, tuple);
    }


    public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
        return out.emit(Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }


    public List<Integer> emit(Tuple anchor, List<Object> tuple) {
        return out.emit(Utils.DEFAULT_STREAM_ID, anchor, tuple);
    }

    public List<Integer> emit(List<Object> tuple) {
        return out.emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    public void emitDirect(int taskId, String streamId, Tuple anchor,
                           List<Object> tuple) {
        out.emitDirect(taskId, streamId, Arrays.asList(anchor), tuple);
    }


    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        out.emitDirect(taskId, streamId, (List) null, tuple);
    }


    public void emitDirect(int taskId, Collection<Tuple> anchors,
                           List<Object> tuple) {
        out.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }


    public void emitDirect(int taskId, Tuple anchor, List<Object> tuple) {
        out.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchor, tuple);
    }

    public void emitDirect(int taskId, List<Object> tuple) {
        out.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }

    public void emitDirect(int taskId, String streamId,
                           Collection<Tuple> anchors, List<Object> tuple) {
        out.emitDirect(taskId, streamId, anchors, tuple);
    }

    public void ack(Tuple input) {
        out.ack(input);
    }

    public void fail(Tuple input) {
        out.fail(input);
    }

    public void reportError(Throwable error) {
        out.reportError(error);
    }

    public void flush() {
        out.flush();
    }


    public List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors,
                                  List<Object> tuple) {
        return ((BoltCollector) (out.getDelegate())).emitCtrl(streamId, anchors, tuple);
    }

    public List<Integer> emitCtrl(String streamId, Tuple anchor, List<Object> tuple) {
        return emitCtrl(streamId, Arrays.asList(anchor), tuple);
    }

    public List<Integer> emitCtrl(String streamId, List<Object> tuple) {
        return emitCtrl(streamId, (List) null, tuple);
    }


    public List<Integer> emitCtrl(Collection<Tuple> anchors, List<Object> tuple) {
        return emitCtrl(Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }


    public List<Integer> emitCtrl(Tuple anchor, List<Object> tuple) {
        return emitCtrl(Utils.DEFAULT_STREAM_ID, Arrays.asList(anchor), tuple);
    }

    public List<Integer> emitCtrl(List<Object> tuple) {
        return emitCtrl(Utils.DEFAULT_STREAM_ID, (List) null, tuple);
    }

    public void emitDirectCtrl(int taskId, String streamId, Tuple anchor,
                               List<Object> tuple) {
        emitDirectCtrl(taskId, streamId, Arrays.asList(anchor), tuple);
    }


    public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple) {
        emitDirectCtrl(taskId, streamId, (List) null, tuple);
    }


    public void emitDirectCtrl(int taskId, Collection<Tuple> anchors,
                               List<Object> tuple) {
        emitDirectCtrl(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple);
    }


    public void emitDirectCtrl(int taskId, Tuple anchor, List<Object> tuple) {
        emitDirectCtrl(taskId, Utils.DEFAULT_STREAM_ID, Arrays.asList(anchor), tuple);
    }

    public void emitDirectCtrl(int taskId, List<Object> tuple) {
        emitDirectCtrl(taskId, Utils.DEFAULT_STREAM_ID, (List) null, tuple);
    }

    public void emitDirectCtrl(int taskId, String streamId,
                               Collection<Tuple> anchors, List<Object> tuple) {
        ((BoltCollector) (out.getDelegate())).emitDirectCtrl(taskId, streamId, anchors, tuple);
    }


}
