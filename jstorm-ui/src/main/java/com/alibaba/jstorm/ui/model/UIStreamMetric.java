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
package com.alibaba.jstorm.ui.model;


import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIStreamMetric extends UIComponentMetric {

    private String streamId;

    public UIStreamMetric(String componentName) {
        super(componentName);
    }

    public UIStreamMetric(String componentName, String streamId){
        this(componentName);
        setStreamId(streamId);
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getStreamId() {
        return streamId;
    }


    @JsonIgnore
    @Override
    public List<ErrorEntity> getErrors() {
        return super.getErrors();
    }

    @JsonIgnore
    @Override
    public String getType() {
        return super.getType();
    }

    @JsonIgnore
    @Override
    public int getParallel() {
        return super.getParallel();
    }

    @JsonIgnore
    @Override
    public String getComponentName() {
        return super.getComponentName();
    }
}
