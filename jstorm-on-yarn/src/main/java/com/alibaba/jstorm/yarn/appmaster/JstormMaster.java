package com.alibaba.jstorm.yarn.appmaster;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.Log4jPropertyHelper;
import com.alibaba.jstorm.yarn.container.ExecutorLoader;
import com.alibaba.jstorm.yarn.context.JstormMasterContext;
import com.alibaba.jstorm.yarn.model.DSEntity;
import com.alibaba.jstorm.yarn.model.DSEvent;
import com.alibaba.jstorm.yarn.model.STARTType;
import com.alibaba.jstorm.yarn.registry.SlotPortsView;
import com.alibaba.jstorm.yarn.server.AMServer;
import com.alibaba.jstorm.yarn.utils.JstormYarnUtils;
import com.alibaba.jstorm.yarn.utils.PortScanner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;

import static com.alibaba.jstorm.yarn.constants.JOYConstants.*;

/**
 * Created by fengjian on 16/4/7.
 * Application master
 */
public class JstormMaster {

    private static final Log LOG = LogFactory.getLog(JstormMaster.class);

    public JstormMasterContext jstormMasterContext = new JstormMasterContext();
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    public AMRMClientAsync amRMClient;

    // In both secure and non-secure modes, this points to the job-submitter.
    @VisibleForTesting
    UserGroupInformation appSubmitterUgi;


    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();
    // Timeline Client
    @VisibleForTesting
    TimelineClient timelineClient;
    private PortScanner portScanner;
    /**
     * The YARN registry service
     */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    public RegistryOperations registryOperations;


    public void killApplicationMaster() {
        jstormMasterContext.done = true;
    }

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            JstormMaster appMaster = new JstormMaster();
            LOG.info("Initializing Jstorm Master!");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(JOYConstants.EXIT_SUCCESS);
            }
            appMaster.run();
            // LRS won't finish at all
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running JstormMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(JOYConstants.EXIT_FAIL1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(JOYConstants.EXIT_SUCCESS);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(JOYConstants.EXIT_FAIL2);
        }
    }


    public JstormMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
        Path jstormyarnConfPath = new Path("jstorm-yarn.xml");
        conf.addResource(jstormyarnConfPath);
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        Options opts = new Options();
        opts.addOption(JOYConstants.APP_ATTEMPT_ID, true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption(JOYConstants.SHELL_SCRIPT, true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption(JOYConstants.CONTAINER_MEMORY, true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption(JOYConstants.CONTAINER_VCORES, true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption(JOYConstants.NUM_CONTAINERS, true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption(JOYConstants.PRIORITY, true, "Application Priority. Default 0");
        opts.addOption(JOYConstants.DEBUG, false, "Dump out debug information");
        opts.addOption(JOYConstants.HELP, false, "Print usage");
        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        try {
            CommandLine cliParser = new GnuParser().parse(opts, args);
            JstormYarnUtils.checkAndSetMasterOptions(cliParser, jstormMasterContext, this.conf);
        } catch (Exception e) {
            LOG.error(e);
        }
        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("com.alibaba.jstorm.yarn.appmaster.JstormMaster", opts);
    }

    /**
     * Build up the port scanner. This may include setting a port range.
     */
    private void buildPortScanner() {
        portScanner = new PortScanner();
        portScanner.setPortRange(JOYConstants.PORT_RANGE);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({"unchecked"})
    public void run() throws Exception {
        LOG.info("Starting JstormMaster");
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        jstormMasterContext.allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName =
                System.getenv(ApplicationConstants.Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);


        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(JOYConstants.AM_RM_CLIENT_INTERVAL, allocListener);
        jstormMasterContext.amRMClient = amRMClient;
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        startTimelineClient(conf);
        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, jstormMasterContext.appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_START, jstormMasterContext.domainId, appSubmitterUgi);
        }


        // Register self with ResourceManager
        // This will start heartbeating to the RM
        jstormMasterContext.appMasterHostname = NetUtils.getHostname();
        //get available port
        buildPortScanner();
        jstormMasterContext.appMasterThriftPort = portScanner.getAvailablePort();

        //since appMasterRpcPort not used yet,  set appMasterRpcPort to appMasterThriftPort
        jstormMasterContext.appMasterRpcPort = jstormMasterContext.appMasterThriftPort;

        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(jstormMasterContext.appMasterHostname, jstormMasterContext.appMasterRpcPort,
                        jstormMasterContext.appMasterTrackingUrl);
        // Dump out information about cluster capability as seen by the
        // resource manager
        jstormMasterContext.maxMemory = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capability of resources in this cluster " + jstormMasterContext.maxMemory);

        jstormMasterContext.maxVcores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capability of resources in this cluster " + jstormMasterContext.maxVcores);

        // A resource ask cannot exceed the max.
        if (jstormMasterContext.containerMemory > jstormMasterContext.maxMemory) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + jstormMasterContext.containerMemory + ", max="
                    + jstormMasterContext.maxMemory);
            jstormMasterContext.containerMemory = jstormMasterContext.maxMemory;
        }

        if (jstormMasterContext.containerVirtualCores > jstormMasterContext.maxVcores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + jstormMasterContext.containerVirtualCores + ", max="
                    + jstormMasterContext.maxVcores);
            jstormMasterContext.containerVirtualCores = jstormMasterContext.maxVcores;
        }

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(jstormMasterContext.appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        jstormMasterContext.numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

        //Setup RegistryOperations
        registryOperations = RegistryOperationsFactory.createInstance(JOYConstants.YARN_REGISTRY, conf);
        setupInitialRegistryPaths();
        registryOperations.start();

        //add previous AM containers to supervisor and nimbus container list
        for (Container container : previousAMRunningContainers) {

            String containerPath = RegistryUtils.componentPath(
                    JOYConstants.APP_TYPE, jstormMasterContext.instanceName,
                    container.getId().getApplicationAttemptId().getApplicationId().toString(), container.getId().toString());
            ServiceRecord sr = null;
            try {
                if (!registryOperations.exists(containerPath)) {

                    String contianerHost = container.getNodeId().getHost();
                    registryOperations.mknode(containerPath, true);
                    sr = new ServiceRecord();
                    sr.set(JOYConstants.HOST, contianerHost);
                    sr.set(YarnRegistryAttributes.YARN_ID, container.getId().toString());
                    sr.description = JOYConstants.CONTAINER;
                    sr.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                            PersistencePolicies.CONTAINER);
                    registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (container.getPriority().getPriority() == 0)
                jstormMasterContext.supervisorContainers.add(container);
            else if (container.getPriority().getPriority() == 1) {
                jstormMasterContext.nimbusContainers.add(container);
            }
        }

        jstormMasterContext.requestBlockingQueue = new LinkedBlockingQueue<ContainerRequest>();

        jstormMasterContext.service_user_name = RegistryUtils.currentUser();

        jstormMasterContext.instanceName = conf.get(JOYConstants.INSTANCE_NAME_KEY);
        this.jstormMasterContext.user = conf.get(JOYConstants.JSTORM_YARN_USER);
        this.jstormMasterContext.password = conf.get(JOYConstants.JSTORM_YARN_PASSWORD);
        this.jstormMasterContext.oldPassword = conf.get(JOYConstants.JSTORM_YARN_OLD_PASSWORD);

        LOG.info("find available port for am rpc server which is : " + jstormMasterContext.appMasterThriftPort);

        String appPath = RegistryUtils.servicePath(
                JOYConstants.APP_TYPE, jstormMasterContext.instanceName, jstormMasterContext.appAttemptID.getApplicationId().toString());
        String instancePath = RegistryUtils.serviceclassPath(
                JOYConstants.APP_TYPE, jstormMasterContext.instanceName);

        LOG.info("Registering application " + jstormMasterContext.appAttemptID.getApplicationId().toString());

        ServiceRecord application = setupServiceRecord();
        jstormMasterContext.nimbusDataDirPrefix = conf.get(JOYConstants.INSTANCE_DATA_DIR_KEY);
        LOG.info("generate instancePath on zk , path is:" + instancePath);

        if (registryOperations.exists(instancePath)) {
            ServiceRecord previousRegister = registryOperations.resolve(instancePath);
            application.set(JOYConstants.NIMBUS_HOST, previousRegister.get(JOYConstants.NIMBUS_HOST, JOYConstants.EMPTY));
            application.set(JOYConstants.NIMBUS_CONTAINER, previousRegister.get(JOYConstants.NIMBUS_CONTAINER, JOYConstants.EMPTY));
            application.set(JOYConstants.NIMBUS_LOCAL_DIR, previousRegister.get(JOYConstants.NIMBUS_LOCAL_DIR, JOYConstants.EMPTY));

            jstormMasterContext.previousNimbusHost = previousRegister.get(JOYConstants.NIMBUS_HOST, "");

            Date now = new Date();
            Map<String, ServiceRecord> apps = RegistryUtils.listServiceRecords(registryOperations, instancePath);
            for (String subAppPath : apps.keySet()) {
                LOG.info("existApp:" + subAppPath);
                ServiceRecord subApp = apps.get(subAppPath);
                Long lastHeatBeatTime = 0l;
                try {
                    lastHeatBeatTime = Long.parseLong(subApp.get(JOYConstants.APP_HEARTBEAT_TIME));
                } catch (Exception e) {
                    LOG.error(e);
                }
                if (now.getTime() - lastHeatBeatTime > 5 * JOYConstants.HEARTBEAT_TIME_INTERVAL
                        || lastHeatBeatTime > now.getTime() || subAppPath.trim().equals(appPath.trim())) {
                    LOG.info("application " + subAppPath + " not response , delete it!");
                    registryOperations.delete(subAppPath, true);
                }
            }
        }

        if (!jstormMasterContext.done) {
            jstormMasterContext.config = conf;
            registryOperations.mknode(appPath, true);
            registryOperations.bind(instancePath, application, BindFlags.OVERWRITE);
            ServiceRecord previousRegister = registryOperations.resolve(instancePath);
            LOG.info("previousRegister:" + previousRegister.toString());
            LOG.info("register path: " + instancePath);
            AMServer as = new AMServer(jstormMasterContext.appMasterThriftPort);
            as.Start(this);
        }
    }

    private ServiceRecord setupServiceRecord() {
        ServiceRecord application = new ServiceRecord();
        application.set(YarnRegistryAttributes.YARN_ID, jstormMasterContext.appAttemptID.getApplicationId().toString());
        application.description = JOYConstants.AM;
        application.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                PersistencePolicies.PERMANENT);
        Map<String, String> addresses = new HashMap<String, String>();
        addresses.put(JOYConstants.HOST, jstormMasterContext.appMasterHostname);
        addresses.put(JOYConstants.PORT, String.valueOf(jstormMasterContext.appMasterThriftPort));
        Endpoint endpoint = new Endpoint(JOYConstants.HTTP, JOYConstants.HOST_PORT, JOYConstants.RPC, addresses);
        application.addExternalEndpoint(endpoint);
        return application;
    }

    void startTimelineClient(final Configuration conf)
            throws YarnException, IOException, InterruptedException {
        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
                        // Creating the Timeline Client
                        timelineClient = TimelineClient.createTimelineClient();
                        timelineClient.init(conf);
                        timelineClient.start();
                    } else {
                        timelineClient = null;
                        LOG.warn("Timeline service is not enabled");
                    }
                    return null;
                }
            });
        } catch (UndeclaredThrowableException e) {
            throw new YarnException(e.getCause());
        }
    }

    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    protected boolean finish() {
        // wait for completion.
        String appPath;
        while (!jstormMasterContext.done
                ) {
            try {
                Thread.sleep(JOYConstants.HEARTBEAT_TIME_INTERVAL);
                appPath = RegistryUtils.servicePath(
                        JOYConstants.APP_TYPE, jstormMasterContext.instanceName, jstormMasterContext.appAttemptID.getApplicationId().toString());
                ServiceRecord app = new ServiceRecord();
                Date now = new Date();
                app.set(JOYConstants.APP_HEARTBEAT_TIME, String.valueOf(now.getTime()));
                registryOperations.bind(appPath, app, BindFlags.OVERWRITE);

            } catch (Exception ex) {
                LOG.error(ex);
            }
        }

        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, jstormMasterContext.appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_END, jstormMasterContext.domainId, appSubmitterUgi);
        }

        appPath = RegistryUtils.servicePath(
                JOYConstants.APP_TYPE, jstormMasterContext.instanceName, jstormMasterContext.appAttemptID.getApplicationId().toString());
        try {
            registryOperations.delete(appPath, true);
            LOG.info("unRegister application' appPath:" + appPath);
        } catch (IOException e) {
            LOG.error("Failed to unRegister application's Registry", e);
        }

        // Join all launched threads
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(JOYConstants.JOIN_THREAD_TIMEOUT);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (jstormMasterContext.numFailedContainers.get() == 0 &&
                jstormMasterContext.numCompletedContainers.get() == jstormMasterContext.numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + jstormMasterContext.numTotalContainers
                    + ", completed=" + jstormMasterContext.numCompletedContainers.get() + ", allocated="
                    + jstormMasterContext.numAllocatedContainers.get() + ", failed="
                    + jstormMasterContext.numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        amRMClient.stop();

        // Stop Timeline Client
        if (timelineClient != null) {
            timelineClient.stop();
        }

        return success;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(jstormMasterContext.appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                Map<Long, Container> nimbusMap = new HashMap<Long, Container>();
                for (Container container : jstormMasterContext.nimbusContainers) {
                    nimbusMap.put(container.getId().getContainerId(), container);
                }
                Map<Long, Container> supervisorMap = new HashMap<Long, Container>();
                for (Container container : jstormMasterContext.supervisorContainers) {
                    supervisorMap.put(container.getId().getContainerId(), container);
                }

                Long containerId = containerStatus.getContainerId().getContainerId();

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (JOYConstants.EXIT_SUCCESS != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        jstormMasterContext.numCompletedContainers.incrementAndGet();
                        jstormMasterContext.numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        jstormMasterContext.numAllocatedContainers.decrementAndGet();
                        jstormMasterContext.numRequestedContainers.decrementAndGet();
                    }

                    if (nimbusMap.containsKey(containerId)) {
                        jstormMasterContext.nimbusContainers.remove(nimbusMap.get(containerId));
                    } else if (supervisorMap.containsKey(containerId)) {
                        jstormMasterContext.supervisorContainers.remove(supervisorMap.get(containerId));
                    }

                } else {
                    //if container over and wasn't killed by framework ,then resend ContainerRequest and launch it again
                    jstormMasterContext.numCompletedContainers.incrementAndGet();
                    LOG.info("process in this Container completed by itself, should restart." + ", containerId="
                            + containerStatus.getContainerId());

                    ContainerRequest containerAsk = null;
                    if (nimbusMap.containsKey(containerId)) {
                        Container nimbusContainer = nimbusMap.get(containerId);
                        containerAsk = setupContainerAskForRM(nimbusContainer.getResource().getMemory(), nimbusContainer.getResource().getVirtualCores(), nimbusContainer.getPriority().getPriority(), nimbusContainer.getNodeId().getHost());
                        LOG.info("restart nimbus container" + ", containerId="
                                + containerStatus.getContainerId());
                    } else if (supervisorMap.containsKey(containerId)) {
                        Container supervisorContainer = supervisorMap.get(containerId);
                        containerAsk = setupContainerAskForRM(supervisorContainer.getResource().getMemory(), supervisorContainer.getResource().getVirtualCores(), supervisorContainer.getPriority().getPriority(), supervisorContainer.getNodeId().getHost());
                        LOG.info("restart supervisor container" + ", containerId="
                                + containerStatus.getContainerId());
                    } else {
                        LOG.info("restart failed. cant find this container in exist queue" + ", containerId="
                                + containerStatus.getContainerId());
                    }
                    if (containerAsk != null) {
                        amRMClient.addContainerRequest(containerAsk);
                        try {
                            jstormMasterContext.requestBlockingQueue.put(containerAsk);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                String containerPath = RegistryUtils.componentPath(
                        JOYConstants.APP_TYPE, jstormMasterContext.instanceName, jstormMasterContext.appAttemptID.getApplicationId().toString(), containerStatus.getContainerId().toString());
                try {
                    if (registryOperations.exists(containerPath)) {
                        registryOperations.delete(containerPath, true);
                    }
                } catch (Exception ex) {
                    LOG.error("error to delete registry of container when container complete", ex);
                }
                if (timelineClient != null) {
                    publishContainerEndEvent(
                            timelineClient, containerStatus, jstormMasterContext.domainId, appSubmitterUgi);
                }
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            jstormMasterContext.numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores"
                        + allocatedContainer.getResource().getVirtualCores());

                // check priority to  assign start type . this priority was assigned by JstormAMHandler
                STARTType startType;
                //todo: register every supervisor containers host
                if (allocatedContainer.getPriority().getPriority() == 0) {

                    String supervisorHost = allocatedContainer.getNodeId().getHost();
                    startType = STARTType.SUPERVISOR;
                    String containerPath = RegistryUtils.componentPath(
                            JOYConstants.APP_TYPE, jstormMasterContext.instanceName,
                            allocatedContainer.getId().getApplicationAttemptId().getApplicationId().toString(), allocatedContainer.getId().toString());

                    ServiceRecord sr = null;

                    try {
                        if (!registryOperations.exists(containerPath)) {
                            registryOperations.mknode(containerPath, true);
                            sr = new ServiceRecord();
                            sr.set(JOYConstants.HOST, supervisorHost);
                            sr.set(YarnRegistryAttributes.YARN_ID, allocatedContainer.getId().toString());
                            sr.description = JOYConstants.CONTAINER;
                            sr.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                                    PersistencePolicies.CONTAINER);
                            registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    startType = STARTType.NIMBUS;
                    // set nimbusHost
                    jstormMasterContext.nimbusHost = allocatedContainer.getNodeId().getHost();
                    String path = RegistryUtils.serviceclassPath(
                            JOYConstants.APP_TYPE, jstormMasterContext.instanceName);

                    // when nimbus restart or failed,we need reload nimbus data in previous nimbus container
                    // so when nimbus container allocated we register nimbus's host, directory and containerId,  pull previous nimbus
                    // data from previous nimbus host if necessary.
                    ServiceRecord serviceRecord = setupServiceRecord();
                    jstormMasterContext.previousNimbusHost = JOYConstants.EMPTY;
                    try {
                        ServiceRecord sr = registryOperations.resolve(path);
                        jstormMasterContext.previousNimbusHost = sr.get(JOYConstants.NIMBUS_HOST, JOYConstants.EMPTY);

                        LOG.info("previousNimbusHost is :" + jstormMasterContext.previousNimbusHost + "; nimbusHost is :" + jstormMasterContext.nimbusHost);
                        //  nimbus location register, then we can restart nimbus with no work loss
                        serviceRecord.set(JOYConstants.NIMBUS_HOST, jstormMasterContext.nimbusHost);
                        serviceRecord.set(JOYConstants.NIMBUS_LOCAL_DIR, jstormMasterContext.nimbusDataDirPrefix);
                        serviceRecord.set(JOYConstants.NIMBUS_CONTAINER, allocatedContainer.getId().toString());
                        registryOperations.bind(path, serviceRecord, BindFlags.OVERWRITE);

                    } catch (Exception ex) {
                        LOG.error(ex);
                    }
                    LOG.info("allocated nimbus container , nimbus host is :" + jstormMasterContext.nimbusHost);
                }

                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener, startType);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();

                // need to remove container request when allocated,
                // otherwise RM will continues allocate container over needs
                if (!jstormMasterContext.requestBlockingQueue.isEmpty()) {
                    try {
                        amRMClient.removeContainerRequest(jstormMasterContext.requestBlockingQueue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void onShutdownRequest() {
            jstormMasterContext.done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
//            float progress = (float) numCompletedContainers.get()
//                    / numTotalContainers;
//            return progress;
            // always be 50%
            return 0.5f;
        }

        @Override
        public void onError(Throwable e) {
            jstormMasterContext.done = true;
            amRMClient.stop();
        }
    }

    @VisibleForTesting
    static class NMCallbackHandler
            implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();
        private final JstormMaster applicationMaster;

        public NMCallbackHandler(JstormMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
            if (applicationMaster.timelineClient != null) {
                JstormMaster.publishContainerStartEvent(
                        applicationMaster.timelineClient, container,
                        applicationMaster.jstormMasterContext.domainId, applicationMaster.appSubmitterUgi);
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.jstormMasterContext.numCompletedContainers.incrementAndGet();
            applicationMaster.jstormMasterContext.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        Container container;

        NMCallbackHandler containerListener;

        STARTType startType;

        /**
         * @param lcontainer        Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainerRunnable(
                Container lcontainer, NMCallbackHandler containerListener, STARTType startType) {
            this.container = lcontainer;
            this.containerListener = containerListener;
            this.startType = startType;
        }

        @Override

        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        public void run() {
            LOG.info("Setting up container launch container for containerid="
                    + container.getId());

            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            // The container for the eventual shell commands needs its own local resources too.
            if (!jstormMasterContext.scriptPath.isEmpty()) {
                Path renamedScriptPath;
                if (Shell.WINDOWS) {
                    renamedScriptPath = new Path(jstormMasterContext.scriptPath + ".bat");
                } else {
                    renamedScriptPath = new Path(jstormMasterContext.scriptPath + ".sh");
                }
                try {
                    // rename the script file based on the underlying OS syntax.
                    renameScriptFile(renamedScriptPath);
                } catch (Exception e) {
                    LOG.error(
                            "Not able to add suffix (.bat/.sh) to the shell script filename",
                            e);
                    //  we cannot continue launching the container. so  release it.
                    jstormMasterContext.numCompletedContainers.incrementAndGet();
                    jstormMasterContext.numFailedContainers.incrementAndGet();
                    return;
                }

                URL yarnUrl;
                try {
                    yarnUrl = ConverterUtils.getYarnUrlFromURI(
                            new URI(renamedScriptPath.toString()));
                } catch (URISyntaxException e) {
                    LOG.error("Error when trying to use shell script path specified"
                            + " in env, path=" + renamedScriptPath, e);
                    return;
                }
                URL jarUrl;
                try {
                    jarUrl = ConverterUtils.getYarnUrlFromURI(
                            new URI(jstormMasterContext.appMasterJarPath.toString()));
                } catch (URISyntaxException e) {
                    LOG.error("Error when trying to use shell script path specified"
                            + " in env, path=" + jstormMasterContext.appMasterJarPath, e);
                    return;
                }
                try {
                    FileSystem fileSystem = FileSystem.get(conf);
                    FileStatus appMasterJarPathStatus = fileSystem.getFileStatus(new Path(jstormMasterContext.appMasterJarPath));
                    jstormMasterContext.jarPathLen = appMasterJarPathStatus.getLen();
                    jstormMasterContext.jarTimestamp = appMasterJarPathStatus.getModificationTime();
                    FileStatus scriptStatus = fileSystem.getFileStatus(renamedScriptPath);
                    jstormMasterContext.shellScriptPathLen = scriptStatus.getLen();
                    jstormMasterContext.shellScriptPathTimestamp = scriptStatus.getModificationTime();
                    LOG.info("jar len:" + jstormMasterContext.jarPathLen + " jar timespan:" + jstormMasterContext.jarTimestamp);

                } catch (IOException e) {
                    LOG.error("get hdfs filestatus"
                            + " in env, path=" + jstormMasterContext.appMasterJarPath, e);
                }

                LocalResource shellRsrc = LocalResource.newInstance(yarnUrl,
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        jstormMasterContext.shellScriptPathLen, jstormMasterContext.shellScriptPathTimestamp);
                localResources.put(JOYConstants.ExecShellStringPath, shellRsrc);

                LocalResource jarRsrc = LocalResource.newInstance(jarUrl,
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        jstormMasterContext.jarPathLen, jstormMasterContext.jarTimestamp);
                localResources.put(JOYConstants.appMasterJarPath, jarRsrc);

                LOG.info(shellRsrc.getResource().getFile());
                LOG.info(jarRsrc.getResource().getFile());

                jstormMasterContext.shellCommand = Shell.WINDOWS ? JOYConstants.windows_command : JOYConstants.linux_bash_command;
            }

            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(9);

            // Set executable command
            vargs.add(jstormMasterContext.shellCommand);
            // Set shell script path
            if (!jstormMasterContext.scriptPath.isEmpty()) {
                vargs.add(Shell.WINDOWS ? JOYConstants.ExecBatScripStringtPath
                        : JOYConstants.ExecShellStringPath);
            }

            String startTypeStr = JOYConstants.SUPERVISOR;
            // start type specified to be excute by shell script to start jstorm process
            if (startType == STARTType.NIMBUS) {
                startTypeStr = JOYConstants.NIMBUS;
                vargs.add(JOYConstants.NIMBUS);
                //put containerId in nimbus containers queue
                try {
                    jstormMasterContext.nimbusContainers.put(this.container);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                vargs.add(JOYConstants.SUPERVISOR);
                try {
                    jstormMasterContext.supervisorContainers.put(this.container);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // pass instanceName for multiple instance deploy
            jstormMasterContext.nimbusDataDirPrefix = conf.get(JOYConstants.INSTANCE_DATA_DIR_KEY);
            String localDir = jstormMasterContext.nimbusDataDirPrefix + container.getId().toString() + JOYConstants.BACKLASH
                    + jstormMasterContext.instanceName;
            vargs.add(localDir);

            vargs.add(jstormMasterContext.deployPath);

            //get superviorhost's free port
            SlotPortsView slotPortsView = new SlotPortsView(jstormMasterContext.instanceName, container.getId(), registryOperations);
            slotPortsView.setMinPort(conf.getInt(JOYConstants.SUPERVISOR_MIN_PORT_KEY, JOYConstants.PORT_RANGE_MIN));
            slotPortsView.setMaxPort(conf.getInt(JOYConstants.SUPERVISOR_MAX_PORT_KEY, JOYConstants.PORT_RANGE_MAX));
            String slotPortsStr = JOYConstants.EMPTY;
            try {
                slotPortsStr = slotPortsView.getSupervisorSlotPorts(container.getResource().getMemory(),
                        container.getResource().getVirtualCores(), container.getNodeId().getHost());

                vargs.add(slotPortsStr);
            } catch (Exception ex) {
                LOG.error("failed get slot ports , container " + container.toString() + "launch fail", ex);
                return;
            }
            String logviewPort = JOYConstants.DEFAULT_LOGVIEW_PORT;
            String nimbusThriftPort = JOYConstants.DEFAULT_NIMBUS_THRIFT_PORT;
            try {
                logviewPort = slotPortsView.getSupervisorSlotPorts(JOYConstants.DEFAULT_SUPERVISOR_MEMORY,
                        JOYConstants.DEFAULT_SUPERVISOR_VCORES, container.getNodeId().getHost());
                nimbusThriftPort = slotPortsView.getSupervisorSlotPorts(JOYConstants.DEFAULT_SUPERVISOR_MEMORY,
                        JOYConstants.DEFAULT_SUPERVISOR_VCORES, container.getNodeId().getHost());
            } catch (Exception e) {
                e.printStackTrace();
            }
            String hadoopHome = conf.get(JOYConstants.HADOOP_HOME_KEY);
            String javaHome = conf.get(JOYConstants.JAVA_HOME_KEY);
            String pythonHome = conf.get(JOYConstants.PYTHON_HOME_KEY);
            vargs.add(hadoopHome);
            vargs.add(javaHome);//$6
            vargs.add(pythonHome);//$7

            String deployDst = conf.get(JOYConstants.INSTANCE_DEPLOY_DEST_KEY);
            if (deployDst == null) {
                deployDst = jstormMasterContext.nimbusDataDirPrefix;
            }
            String dstPath = deployDst + container.getId().toString();
            vargs.add(dstPath);//$8

            // Set args for the shell command if any
            vargs.add(jstormMasterContext.shellArgs);
            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();

            Map<String, String> envs = System.getenv();
            String exectorCommand = ExecutorLoader.loadCommand(jstormMasterContext.instanceName, jstormMasterContext.shellCommand, startTypeStr,
                    this.container.getId().toString(), localDir, jstormMasterContext.deployPath, hadoopHome, javaHome, pythonHome, dstPath, slotPortsStr, jstormMasterContext.shellArgs,
                    envs.get(JOYConstants.CLASS_PATH), JOYConstants.ExecShellStringPath, jstormMasterContext.appAttemptID.getApplicationId().toString(), logviewPort, nimbusThriftPort);

            exectorCommand = exectorCommand + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                    + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

            commands.add(exectorCommand.toString());

            LOG.info("a container command is :" + exectorCommand.toString());
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, jstormMasterContext.shellEnv, commands, null, jstormMasterContext.allTokens.duplicate(), null);
            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    private void renameScriptFile(final Path renamedScriptPath)
            throws IOException, InterruptedException {
        appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
                FileSystem fs = renamedScriptPath.getFileSystem(conf);
                fs.rename(new Path(jstormMasterContext.scriptPath), renamedScriptPath);
                return null;
            }
        });
        LOG.info("User " + appSubmitterUgi.getUserName()
                + " added suffix(.sh/.bat) to script file as " + renamedScriptPath);
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    public ContainerRequest setupContainerAskForRM(int containerMemory, int containerVirtualCores, int priority, String host) {
        // setup requirements for hosts
        // using * as any host will do for the jstorm app
        // set the priority for the request
        Priority pri = Priority.newInstance(priority);
        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(containerMemory,
                containerVirtualCores);

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        LOG.info("By Thrift Server Requested container  ask: " + request.toString());
        return request;
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    public ContainerRequest setupContainerAskForRM(int containerMemory, int containerVirtualCores, int priority, String[] racks, String[] hosts) {
        Priority pri = Priority.newInstance(priority);
        Resource capability = Resource.newInstance(containerMemory,
                containerVirtualCores);
        ContainerRequest request = new ContainerRequest(capability, hosts, racks,
                pri, false);
        LOG.info("By Thrift Server Requested container  ask: " + request.toString());
        return request;
    }


    private static void publishContainerStartEvent(
            final TimelineClient timelineClient, Container container, String domainId,
            UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter(JOYConstants.USER, ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_START.toString());
        event.addEventInfo(JOYConstants.NODE, container.getNodeId().toString());
        event.addEventInfo(JOYConstants.RESOURCES, container.getResource().toString());
        entity.addEvent(event);

        try {
            ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
                @Override
                public TimelinePutResponse run() throws Exception {
                    return timelineClient.putEntities(entity);
                }
            });
        } catch (Exception e) {
            LOG.error("Container start event could not be published for "
                            + container.getId().toString(),
                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
        }
    }

    private static void publishContainerEndEvent(
            final TimelineClient timelineClient, ContainerStatus container,
            String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getContainerId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter(JOYConstants.USER, ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_END.toString());
        event.addEventInfo(JOYConstants.STATE, container.getState().name());
        event.addEventInfo(JOYConstants.EXIT_STATE, container.getExitStatus());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (YarnException e) {
            LOG.error("Container end event could not be published for "
                    + container.getContainerId().toString(), e);
        } catch (IOException e) {
            LOG.error("Container end event could not be published for "
                    + container.getContainerId().toString(), e);
        }
    }

    private static void publishApplicationAttemptEvent(
            final TimelineClient timelineClient, String appAttemptId,
            DSEvent appEvent, String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(appAttemptId);
        entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter(JOYConstants.USER, ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (YarnException e) {
            LOG.error("App Attempt "
                    + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? JOYConstants.START : JOYConstants.END)
                    + " event could not be published for "
                    + appAttemptId.toString(), e);
        } catch (IOException e) {
            LOG.error("App Attempt "
                    + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? JOYConstants.START : JOYConstants.END)
                    + " event could not be published for "
                    + appAttemptId.toString(), e);
        }
    }


    /**
     * TODO: purge this once RM is doing the work
     *
     * @throws IOException
     */
    protected void setupInitialRegistryPaths() throws IOException {
        if (registryOperations instanceof RMRegistryOperationsService) {
            RMRegistryOperationsService rmRegOperations =
                    (RMRegistryOperationsService) registryOperations;
            rmRegOperations.initUserRegistryAsync(jstormMasterContext.service_user_name);
        }
    }

}