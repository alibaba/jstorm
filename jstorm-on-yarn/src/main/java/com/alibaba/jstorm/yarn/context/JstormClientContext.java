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
package com.alibaba.jstorm.yarn.context;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by fengjian on 16/4/7.
 */
public class JstormClientContext implements MasterContext {
    public JstormClientContext(){

    }
    // Configuration
    public Configuration conf;
    public YarnClient yarnClient;
    // Application master specific info to register a new Application with RM/ASM
    public String appName = "";
    // App master priority
    public int amPriority = 0;
    // Queue for App master
    public String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    public int amMemory = 10000;
    // Amt. of virtual core resource to request for to run the App Master
    public int amVCores = 1;

    // Application master jar file
    public String appMasterJar = "";
    public String libJars = "";
    public String homeDir = "";
    public String confFile = "";
    public String hadoopConfDir = "";
    public String rmHost = "";
    public String nameNodeHost = "";
    public String deployPath = "";
    public String instanceName = "";
    // Shell command to be executed
    public String shellCommand = "";
    // Location of shell script
    public String shellScriptPath = "";
    // Args to be passed to the shell command
    public String[] shellArgs = new String[]{};
    // Env variables to be setup for the shell command
    public Map<String, String> shellEnv = new HashMap<String, String>();
    // Shell Command Container prioritsy
    public int shellCmdPriority = 0;

    // Amt of memory to request for container in which shell script will be executed
    public int containerMemory = 10;
    // Amt. of virtual cores to request for container in which shell script will be executed
    public int containerVirtualCores = 1;
    // No. of containers in which the shell script needs to be executed
    public int numContainers = 1;
    public String nodeLabelExpression = null;

    // log4j.properties file
    // if available, add to local resources and set into classpath
    public String log4jPropFile = "";

    // Start time for client
    public final long clientStartTime = System.currentTimeMillis();
    // Timeout threshold for client. Kill app after time interval expires.
    // LRS does't need this
    public long clientTimeout = 100;

    // flag to indicate whether to keep containers across application attempts.
    // LongRunningService should set this true
    public boolean keepContainers = true;

    // need to set for LRS, otherwise AM retries will be limited
    // time unit is micro seconds
    public long attemptFailuresValidityInterval = 10;

    // Debug flag
    public boolean debugFlag = false;

    // Timeline domain ID
    public String domainId = null;

    // Flag to indicate whether to create the domain of the given ID
    public boolean toCreateDomain = false;

    // Timeline domain reader access control
    public String viewACLs = null;

    // Timeline domain writer access control
    public String modifyACLs = null;

    // Command line options
    public Options opts;


    @Override
    public String getUser() {
        return null;
    }

    @Override
    public Credentials getCredentials() {
        return null;
    }

    @Override
    public ApplicationId getApplicationID() {
        return null;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
        return null;
    }

    @Override
    public ContainerId getContainerId() {
        return null;
    }

    @Override
    public long getSubmitTime() {
        return 0;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public String getAddress() {
        return null;
    }

    @Override
    public Configuration getYarnConfiguration() {
        return conf;
    }

    @Override
    public BlockingQueue<Container> getContainers() {
        return null;
    }

    @Override
    public BlockingQueue<Container> getSupervisorContainers() {
        return null;
    }

    @Override
    public BlockingQueue<Container> getNimbusContainers() {
        return null;
    }

    @Override
    public BlockingQueue<AMRMClient.ContainerRequest> getContainerRequest() {
        return null;
    }

    @Override
    public Set<String> getUpgradingContainerIds() {
        return null;
    }

    @Override
    public Map<String, String> getShellEnv() {
        return shellEnv;
    }
}
