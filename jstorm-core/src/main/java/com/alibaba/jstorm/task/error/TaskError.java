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

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + code;
		result = prime * result + durationSecs;
		result = prime * result + ((error == null) ? 0 : error.hashCode());
		result = prime * result + ((level == null) ? 0 : level.hashCode());
		result = prime * result + timSecs;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TaskError other = (TaskError) obj;
		if (code != other.code)
			return false;
		if (durationSecs != other.durationSecs)
			return false;
		if (error == null) {
			if (other.error != null)
				return false;
		} else if (!error.equals(other.error))
			return false;
		if (level == null) {
			if (other.level != null)
				return false;
		} else if (!level.equals(other.level))
			return false;
		if (timSecs != other.timSecs)
			return false;
		return true;
	}
    
    

    @Override
    public String toString() {
        
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

	
}
