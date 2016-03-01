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
package com.alibaba.jstorm.task.error;

import java.io.Serializable;

/**
 * Task error stored in Zk(/storm-zk-root/taskerrors/{topologyid}/{taskid})
 * 
 * @author yannian
 * 
 */
public class TaskError implements Serializable {

    private static final long serialVersionUID = 5028789764629555542L;
    private String error;
    private String level;
    private int code;
    private int timSecs;
    private int durationSecs;

    public TaskError(String error, String level, int code, int timSecs) {
        this.error = error;
        this.level = level;
        this.code = code;
        this.timSecs = timSecs;
        this.durationSecs = ErrorConstants.DURATION_SECS_DEFAULT;
    }

    public TaskError(String error, String level, int code, int timSecs, int durationSecs) {
        this.error = error;
        this.level = level;
        this.code = code;
        this.timSecs = timSecs;
        this.durationSecs = durationSecs;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public int getTimSecs() {
        return timSecs;
    }

    public void setTimSecs(int timSecs) {
        this.timSecs = timSecs;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getDurationSecs() {
        return durationSecs;
    }

    public void setDurationSecs(int durationSecs) {
        this.durationSecs = durationSecs;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TaskError)) {
            return false;
        } else if (obj == this) {
            return true;
        } else {
            TaskError taskError = (TaskError) obj;
            if (taskError.getError().equals(error)
                    && taskError.getCode() == code
                    && taskError.getLevel().equals(level)
                    && taskError.getTimSecs() == timSecs
                    && taskError.getDurationSecs() == durationSecs) {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public String toString() {
        return "TaskError{" +
                "error='" + error + '\'' +
                ", level='" + level + '\'' +
                ", code=" + code +
                ", timSecs=" + timSecs +
                ", durationSecs=" + durationSecs +
                '}';
    }
}
