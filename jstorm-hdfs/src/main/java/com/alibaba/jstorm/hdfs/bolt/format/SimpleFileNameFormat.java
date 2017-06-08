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
package com.alibaba.jstorm.hdfs.bolt.format;

import backtype.storm.task.TopologyContext;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


public class SimpleFileNameFormat implements FileNameFormat {

    private static final long serialVersionUID = 1L;

    private String componentId;
    private int taskId;
    private String host;
    private String path = "/storm";
    private String name = "$TIME.$NUM.txt";
    private String timeFormat = "yyyyMMddHHmmss";

    @Override
    public String getName(long rotation, long timeStamp) {
        // compile parameters
        SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormat);
        String ret = name
                .replace("$TIME", dateFormat.format(new Date(timeStamp)))
                .replace("$NUM", String.valueOf(rotation))
                .replace("$HOST", host)
                .replace("$COMPONENT", componentId)
                .replace("$TASK", String.valueOf(taskId));
        return ret;
    }

    @Override
    public String getPath() {
        return path;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
        try {
            this.host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public SimpleFileNameFormat withPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * support parameters:<br/>
     * $TIME - current time. use <code>withTimeFormat</code> to format.<br/>
     * $NUM - rotation number<br/>
     * $HOST - local host name<br/>
     * $COMPONENT - component id<br/>
     * $TASK - task id<br/>
     * 
     * @param name
     *            file name
     * @return
     */
    public SimpleFileNameFormat withName(String name) {
        this.name = name;
        return this;
    }

    public SimpleFileNameFormat withTimeFormat(String timeFormat) {
        //check format
        try{
            new SimpleDateFormat(timeFormat);
        }catch (Exception e) {
            throw new IllegalArgumentException("invalid timeFormat: "+e.getMessage());
        }
        this.timeFormat = timeFormat;
        return this;
    }

}
