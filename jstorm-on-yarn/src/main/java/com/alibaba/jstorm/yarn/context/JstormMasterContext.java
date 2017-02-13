package com.alibaba.jstorm.yarn.context;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
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
public class JstormMasterContext implements MasterContext {

    private ContainerId containerId;
    private ApplicationAttemptId attemptId;
    private Credentials credentials;
    public Configuration config;
    private long submitTime;
    private String address;
    public BlockingQueue<Container> nimbusContainers = new LinkedBlockingQueue<Container>();
    public BlockingQueue<Container> supervisorContainers = new LinkedBlockingQueue<Container>();
    // Handle to communicate with the Resource Manager
    public AMRMClientAsync amRMClient;
    public BlockingQueue<AMRMClient.ContainerRequest> requestBlockingQueue;

    public ApplicationAttemptId appAttemptID;

    // Hostname of the container
    public String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    public int appMasterRpcPort = -1;

    public int appMasterThriftPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    public String appMasterTrackingUrl = "";

    public int maxMemory = 0;
    public int maxVcores = 0;

    // App Master configuration
    // No. of containers to run shell command on
    public int numTotalContainers = 1;
    // Memory to request for the container on which the shell command will run
    public int containerMemory = 2000;
    // VirtualCores to request for the container on which the shell command will run
    public int containerVirtualCores = 1;

    // Counter for completed containers ( complete denotes successful or failed )
    public AtomicInteger numCompletedContainers = new AtomicInteger();
    public AtomicInteger numAllocatedContainers = new AtomicInteger();
    public AtomicInteger numRequestedContainers = new AtomicInteger();
    // Count of failed containers
    public AtomicInteger numFailedContainers = new AtomicInteger();
    // Shell command to be executed
    public String shellCommand = "";
    // Args to be passed to the shell command
    public String shellArgs = "";
    // Env variables to be setup for the shell command
    public Map<String, String> shellEnv = new HashMap<String, String>();

    // Location of shell script ( obtained from info set in env )
    // Shell script path in fs
    public String scriptPath = "";

    public String appMasterJarPath = "";
    // Timestamp needed for creating a local resource
    public long shellScriptPathTimestamp = 0;
    public long jarTimestamp = 0;
    // File length needed for local resource
    public long shellScriptPathLen = 0;
    public long jarPathLen = 0;

    // Timeline domain ID
    public String domainId = null;
    public volatile boolean done;
    public ByteBuffer allTokens;


    public String service_user_name;

    public String instanceName = "";
    public String nimbusDataDirPrefix = "";
    public String nimbusHost = "";
    public String previousNimbusHost = "";

    public String user = "";
    public String password = "";
    public String oldPassword = "";

    public String deployPath = "";

    public InetSocketAddress rpcServiceAddress;

    public JstormMasterContext() {
    }

    public JstormMasterContext(String user, ContainerId containerId,
                               ApplicationAttemptId applicationAttemptId,
                               long appSubmitTime, String nodeHostString,
                               Configuration yarnConfig) {
        this.user = user;
        this.containerId = containerId;
        this.attemptId = applicationAttemptId;
        this.credentials = new Credentials();
        this.submitTime = appSubmitTime;
        this.address = nodeHostString;
        this.config = yarnConfig;
    }

    @Override
    public String getUser() {
        return this.user;
    }

    @Override
    public Credentials getCredentials() {
        return this.credentials;
    }

    @Override
    public ApplicationId getApplicationID() {
        return this.attemptId.getApplicationId();
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
        return this.attemptId;
    }

    @Override
    public ContainerId getContainerId() {
        return this.containerId;
    }

    @Override
    public long getSubmitTime() {
        return this.submitTime;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    @Override
    public Configuration getYarnConfiguration() {
        return this.config;
    }

    @Override
    public BlockingQueue<Container> getContainers() {
        return null;
    }

    @Override
    public BlockingQueue<Container> getSupervisorContainers() {
        return this.supervisorContainers;
    }

    @Override
    public BlockingQueue<Container> getNimbusContainers() {
        return this.nimbusContainers;
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
