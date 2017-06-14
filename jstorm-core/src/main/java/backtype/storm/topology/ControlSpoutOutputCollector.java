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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.task.execute.spout.SpoutCollector;

import java.util.List;

/**
 * callbacks like ICollectorCallback will be unnecessary since control messages are sent sequentially
 *
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ControlSpoutOutputCollector implements IControlSpoutOutputCollector {
    private SpoutOutputCollector out;

    public ControlSpoutOutputCollector(SpoutOutputCollector out) {
        this.out = out;
    }

    public List<Integer> emit(String streamId, List<Object> tuple,
                              Object messageId) {
        return out.emit(streamId, tuple, messageId);
    }

    public List<Integer> emit(List<Object> tuple, Object messageId) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }

    public List<Integer> emit(List<Object> tuple) {
        return emit(tuple, null);
    }


    public List<Integer> emit(String streamId, List<Object> tuple) {
        return emit(streamId, tuple, null);
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple,
                           Object messageId) {
        out.emitDirect(taskId, streamId, tuple, messageId);
    }


    public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        emitDirect(taskId, streamId, tuple, null);
    }

    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, tuple, null);
    }

    public void reportError(Throwable error) {
        out.reportError(error);
    }

    public void flush() {
        out.flush();
    }

    public List<Integer> emitCtrl(String streamId, List<Object> tuple,
                                  Object messageId) {
        return ((SpoutCollector) (out.getDelegate())).emitCtrl(streamId, tuple, messageId);
    }

    public List<Integer> emitCtrl(List<Object> tuple, Object messageId) {
        return emitCtrl(Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }

    public List<Integer> emitCtrl(List<Object> tuple) {
        return emitCtrl(tuple, null);
    }


    public List<Integer> emitCtrl(String streamId, List<Object> tuple) {
        return emitCtrl(streamId, tuple, null);
    }

    public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple,
                               Object messageId) {
        ((SpoutCollector) (out.getDelegate())).emitDirectCtrl(taskId, streamId, tuple, messageId);
    }


    public void emitDirectCtrl(int taskId, List<Object> tuple, Object messageId) {
        emitDirectCtrl(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
    }

    public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple) {
        emitDirectCtrl(taskId, streamId, tuple, null);
    }

    public void emitDirectCtrl(int taskId, List<Object> tuple) {
        emitDirectCtrl(taskId, tuple, null);
    }
}
