package com.alibaba.jstorm.yarn.constants; /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JOYConstants {

    /**
     * Environment key name pointing to the shell script's location
     */
    public static final String DISTRIBUTEDSHELLSCRIPTLOCATION = "DISTRIBUTEDSHELLSCRIPTLOCATION";

    public static final String APPMASTERJARSCRIPTLOCATION = "APPMASTERJARSCRIPTLOCATION";

    public static final String BINARYFILEDEPLOYPATH = "BINARYFILEDEPLOYPATH";

    public static final String INSTANCENAME = "INSTANCENAME";
    /**
     * Environment key name denoting the file timestamp for the shell script.
     * Used to validate the local resource.
     */
    public static final String DISTRIBUTEDSHELLSCRIPTTIMESTAMP = "DISTRIBUTEDSHELLSCRIPTTIMESTAMP";
    public static final String APPMASTERTIMESTAMP = "APPMASTERTIMESTAMP";

    /**
     * Environment key name denoting the file content length for the shell script.
     * Used to validate the local resource.
     */
    public static final String DISTRIBUTEDSHELLSCRIPTLEN = "DISTRIBUTEDSHELLSCRIPTLEN";
    public static final String APPMASTERLEN = "APPMASTERLEN";

    /**
     * Environment key name denoting the timeline domain ID.
     */
    public static final String DISTRIBUTEDSHELLTIMELINEDOMAIN = "DISTRIBUTEDSHELLTIMELINEDOMAIN";


    public static final String CONTAINER_SUPERVISOR_HEARTBEAT = "container.supervisor.heartbeat";

    public static final String CONTAINER_NIMBUS_HEARTBEAT = "container.nimbus.heartbeat";

    public static final int EXECUTOR_HEARTBEAT_TIMEOUT = 60 * 1000;

    public static final int HOST_LOCK_TIMEOUT = 60 * 1000;

    public static final int PORT_RANGE_MIN = 9000;
    public static final int PORT_RANGE_MAX = 15000;


}