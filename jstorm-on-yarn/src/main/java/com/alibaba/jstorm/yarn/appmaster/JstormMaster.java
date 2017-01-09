package com.alibaba.jstorm.yarn.appmaster;

import com.alibaba.jstorm.yarn.JstormOnYarn;
import com.alibaba.jstorm.yarn.Log4jPropertyHelper;
import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.constants.JstormKeys;
import com.alibaba.jstorm.yarn.container.ExecutorLoader;
import com.alibaba.jstorm.yarn.context.JstormMasterContext;
import com.alibaba.jstorm.yarn.registry.SlotPortsView;
import com.alibaba.jstorm.yarn.server.AMServer;
import com.alibaba.jstorm.yarn.utils.PortScanner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
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
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
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
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

/**
 * An ApplicationMaster for executing shell commands on a set of launched
 * containers using the YARN framework.
 * <p/>
 * <p>
 * This class is meant to act as an example on how to write yarn-based
 * application masters.
 * </p>
 * <p/>
 * <p>
 * The com.alibaba.jstorm.yarn.appmaster.JstormMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * com.alibaba.jstorm.yarn.appmaster.JstormMaster is listening on to provide any form of functionality to a
 * client as well as a tracking url that a client can use to keep track of
 * status/job history if needed. However, in the com.alibaba.jstorm.yarn.JstormOnYarn, trackingurl
 * and appMasterHost:appMasterRpcPort are not supported.
 * </p>
 * <p/>
 * <p>
 * The <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> acts as a heartbeat.
 * <p/>
 * <p>
 * For the actual handling of the job, the <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 * <p/>
 * <p>
 * For each allocated container, the <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManagementProtocol} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * <p/>
 * <p/>
 * The <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link ApplicationMasterProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManagementProtocol} by querying for the status of the allocated
 * container's {@link ContainerId}.
 * <p/>
 * <p/>
 * After the job has been completed, the <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>com.alibaba.jstorm.yarn.appmaster.JstormMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JstormMaster {

    private static final Log LOG = LogFactory.getLog(JstormMaster.class);

    @VisibleForTesting
    @Private
    public enum DSEvent {
        DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
    }

    @VisibleForTesting
    @Private
    public enum DSEntity {
        DS_APP_ATTEMPT, DS_CONTAINER
    }

    @VisibleForTesting
    @Private
    public enum STARTType {
        NIMBUS, SUPERVISOR
    }

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    public AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;

    // In both secure and non-secure modes, this points to the job-submitter.
    @VisibleForTesting
    UserGroupInformation appSubmitterUgi;

    public JstormMasterContext jstormMasterContext;

    private YarnClient yarnClient;
    private final List<NodeReport> nodeReports = Lists.newArrayList();
    private long lastNodesRefreshTime;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;

    public BlockingQueue<ContainerRequest> requestBlockingQueue;
    public BlockingQueue<Container> nimbusContainers = new LinkedBlockingQueue<Container>();
    public BlockingQueue<Container> supervisorContainers = new LinkedBlockingQueue<Container>();

    // Application Attempt Id ( combination of attemptId and fail count )
    @VisibleForTesting
    protected ApplicationAttemptId appAttemptID;

    // Hostname of the container
    public String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    public int appMasterRpcPort = -1;

    public int appMasterThriftPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    public int maxMemory = 0;
    public int maxVcores = 0;

    // App Master configuration
    // No. of containers to run shell command on
    @VisibleForTesting
    public int numTotalContainers = 1;
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 2000;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;
    // Priority of the request
    private int requestPriority;

    // Counter for completed containers ( complete denotes successful or failed )
    public AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    @VisibleForTesting
    public AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    public AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    @VisibleForTesting
    public AtomicInteger numRequestedContainers = new AtomicInteger();

    // Shell command to be executed
    private String shellCommand = "";
    // Args to be passed to the shell command
    private String shellArgs = "";
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<String, String>();

    // Location of shell script ( obtained from info set in env )
    // Shell script path in fs
    private String scriptPath = "";

    private String appMasterJarPath = "";
    // Timestamp needed for creating a local resource
    private long shellScriptPathTimestamp = 0;
    private long jarTimestamp = 0;
    // File length needed for local resource
    private long shellScriptPathLen = 0;
    private long jarPathLen = 0;

    // Timeline domain ID
    private String domainId = null;


    // Hardcoded path to shell script in launch container's local env
    private static final String ExecShellStringPath = JstormOnYarn.SCRIPT_PATH + ".sh";
    private static final String ExecBatScripStringtPath = JstormOnYarn.SCRIPT_PATH
            + ".bat";

    // Hardcoded path to custom log_properties
    private static final String log4jPath = "log4j.properties";

    private static final String shellCommandPath = "shellCommands";
    private static final String shellArgsPath = "shellArgs";

    private volatile boolean done;

    private ByteBuffer allTokens;

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<>();

    // Timeline Client
    @VisibleForTesting
    TimelineClient timelineClient;

    private static final String linux_bash_command = "bash";
    private static final String windows_command = "cmd /c";

    private PortScanner portScanner;
    /**
     * The YARN registry service
     */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    public RegistryOperations registryOperations;

    private String service_user_name;

    public String instanceName = "";
    public String nimbusDataDirPrefix = "";
    public String nimbusHost = "";
    public String previousNimbusHost = "";

    public String user = "";
    public String password = "";
    public String oldPassword = "";

    public String deployPath = "";

    private InetSocketAddress rpcServiceAddress;

    public void killApplicationMaster() {
        done = true;
    }

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            JstormMaster appMaster = new JstormMaster();
            LOG.info("Initializing com.alibaba.jstorm.yarn.appmaster.JstormMaster!");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            // LRS won't finish at all
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running com.alibaba.jstorm.yarn.appmaster.JstormMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private void dumpOutDebugInfo() {
        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        BufferedReader buf = null;
        try {
            String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
                    Shell.execCommand("ls", "-al");
            buf = new BufferedReader(new StringReader(lines));
            String line;
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
        } catch (IOException e) {
            LOG.error("Error", e);
        } finally {
            IOUtils.cleanup(LOG, buf);
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
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("base_tracking_url", false, "Base tracking url for application master");

        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        //Check whether customer log4j.properties file exists
        if (fileExist(log4jPath)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(JstormMaster.class,
                        log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }


        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        if (!fileExist(shellCommandPath)
                && envs.get(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION).isEmpty()) {
            throw new IllegalArgumentException(
                    "No shell command or shell script specified to be executed by application master");
        }

        if (fileExist(shellCommandPath)) {
            shellCommand = readContent(shellCommandPath);
        }

        if (fileExist(shellArgsPath)) {
            shellArgs = readContent(shellArgsPath);
        }

        if (cliParser.hasOption("shell_env")) {
            String shellEnvs[] = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }

        if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
            scriptPath = envs.get(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

            appMasterJarPath = envs.get(JOYConstants.APPMASTERJARSCRIPTLOCATION);
            if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
                shellScriptPathTimestamp = Long.parseLong(envs
                        .get(JOYConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
                jarTimestamp = Long.parseLong(envs
                        .get(JOYConstants.APPMASTERTIMESTAMP));
            }
            if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
                shellScriptPathLen = Long.parseLong(envs
                        .get(JOYConstants.DISTRIBUTEDSHELLSCRIPTLEN));
                jarPathLen = Long.parseLong(envs
                        .get(JOYConstants.APPMASTERLEN));
            }

            if (!scriptPath.isEmpty()
                    && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
                LOG.error("Illegal values in env for shell script path" + ", path="
                        + scriptPath + ", len=" + shellScriptPathLen + ", timestamp="
                        + shellScriptPathTimestamp);
                throw new IllegalArgumentException(
                        "Illegal values in env for shell script path");
            }
        }

        if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN)) {
            domainId = envs.get(JOYConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN);
        }

        if (envs.containsKey(JOYConstants.BINARYFILEDEPLOYPATH)
                && !envs.get(JOYConstants.BINARYFILEDEPLOYPATH).equals("")) {
            conf.set("jstorm.yarn.instance.deploy.dir", envs.get(JOYConstants.BINARYFILEDEPLOYPATH));
            deployPath = envs.get(JOYConstants.BINARYFILEDEPLOYPATH);
        }

        if (envs.containsKey(JOYConstants.INSTANCENAME)
                && !envs.get(JOYConstants.INSTANCENAME).equals("")) {
            conf.set("jstorm.yarn.instance.name", envs.get(JOYConstants.INSTANCENAME));
            instanceName = envs.get(JOYConstants.INSTANCENAME);

            if (cliParser.hasOption("base_tracking_url")) {
                appMasterTrackingUrl = cliParser.getOptionValue("base_tracking_url") +
                        "/cluster.htm?cluster_name=" + instanceName;
            }
        }
        LOG.info("deploypath:" + deployPath);
        LOG.info("instanceName:" + instanceName);

//        containerMemory = Integer.parseInt(cliParser.getOptionValue(
//                "container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
                "container_vcores", "1"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
                "num_containers", "1"));
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run distributed shell with no containers");
        }
        requestPriority = Integer.parseInt(cliParser
                .getOptionValue("priority", "0"));
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
//        String portRange = instanceDefinition.
//                getAppConfOperations().getGlobalOptions().
//                getOption(SliderKeys.KEY_ALLOWED_PORT_RANGE, "0");
        String portRange = "9111-9999";
        portScanner.setPortRange(portRange);
    }

    public List<NodeReport> getNodeReports() {
        List<NodeReport> ret = Lists.newArrayListWithCapacity(nodeReports.size());
        synchronized (nodeReports) {
            for (NodeReport node : nodeReports) {
                NodeReport copy = NodeReport.newInstance(node.getNodeId(), node.getNodeState(),
                        node.getHttpAddress(), node.getRackName(), node.getUsed(),
                        node.getCapability(), node.getNumContainers(), node.getHealthReport(),
                        node.getLastHealthReportTime());
                ret.add(copy);
            }
        }
        return ret;
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

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
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
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName =
                System.getenv(ApplicationConstants.Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        startTimelineClient(conf);
        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);
        }


        // Register self with ResourceManager
        // This will start heartbeating to the RM
        appMasterHostname = NetUtils.getHostname();
        //get available port
        buildPortScanner();
        appMasterThriftPort = portScanner.getAvailablePort();

        //since appMasterRpcPort not used yet,  set appMasterRpcPort to appMasterThriftPort
        appMasterRpcPort = appMasterThriftPort;

        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);
        // Dump out information about cluster capability as seen by the
        // resource manager
        maxMemory = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capability of resources in this cluster " + maxMemory);

        maxVcores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capability of resources in this cluster " + maxVcores);

        // A resource ask cannot exceed the max.
        if (containerMemory > maxMemory) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerMemory + ", max="
                    + maxMemory);
            containerMemory = maxMemory;
        }

        if (containerVirtualCores > maxVcores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                    + maxVcores);
            containerVirtualCores = maxVcores;
        }

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

        //Setup RegistryOperations
        registryOperations = RegistryOperationsFactory.createInstance("YarnRegistry", conf);
        setupInitialRegistryPaths();
        registryOperations.start();

        //add previous AM containers to supervisor and nimbus container list
        for (Container container : previousAMRunningContainers) {

            String containerPath = RegistryUtils.componentPath(
                    JstormKeys.APP_TYPE, instanceName,
                    container.getId().getApplicationAttemptId().getApplicationId().toString(), container.getId().toString());
            ServiceRecord sr = null;
            try {
                if (!registryOperations.exists(containerPath)) {

                    String contianerHost = container.getNodeId().getHost();
                    registryOperations.mknode(containerPath, true);
                    sr = new ServiceRecord();
                    sr.set("host", contianerHost);
                    sr.set(YarnRegistryAttributes.YARN_ID, container.getId().toString());
                    sr.description = "container";
                    sr.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                            PersistencePolicies.CONTAINER);
                    registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (container.getPriority().getPriority() == 0)
                supervisorContainers.add(container);
            else if (container.getPriority().getPriority() == 1) {
                nimbusContainers.add(container);
            }
        }

        requestBlockingQueue = new LinkedBlockingQueue<ContainerRequest>();

        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();

        service_user_name = RegistryUtils.currentUser();

        LOG.info("configuration : " + conf.toString());
        LOG.info("configuration value : " + conf.get("hadoop.registry.zk.retry.interval.ms"));

        instanceName = conf.get("jstorm.yarn.instance.name");
        this.user = conf.get("jstorm.yarn.user");
        this.password = conf.get("jstorm.yarn.password");
        this.oldPassword = conf.get("jstorm.yarn.oldpassword");


//        appMasterThriftPort = Integer.parseInt(JstormYarnUtils.getSupervisorPorts(1, 1, appMasterHostname, registryOperations).get(0));

        LOG.info("find available port for am rpc server which is : " + appMasterThriftPort);


        String appPath = RegistryUtils.servicePath(
                JstormKeys.APP_TYPE, instanceName, appAttemptID.getApplicationId().toString());
        String instancePath = RegistryUtils.serviceclassPath(
                JstormKeys.APP_TYPE, instanceName);

        LOG.info("Registering application " + appAttemptID.getApplicationId().toString());

        ServiceRecord application = setupServiceRecord();
        nimbusDataDirPrefix = conf.get("jstorm.yarn.instance.dataDir");
//        deployPath = conf.get("jstorm.yarn.instance.deploy.dir", "");

        LOG.info("instancePath:" + instancePath);
        LOG.info("appPath:" + appPath);

        if (registryOperations.exists(instancePath)) {
            ServiceRecord previousRegister = registryOperations.resolve(instancePath);
            application.set("nimbus.host", previousRegister.get("nimbus.host", ""));
            application.set("nimbus.containerId", previousRegister.get("nimbus.containerId", ""));
            application.set("nimbus.localdir", previousRegister.get("nimbus.localdir", ""));

            previousNimbusHost = previousRegister.get("nimbus.host", "");

            Date now = new Date();
            Map<String, ServiceRecord> apps = RegistryUtils.listServiceRecords(registryOperations, instancePath);
            for (String subAppPath : apps.keySet()) {
                LOG.info("existApp:" + subAppPath);
                ServiceRecord subApp = apps.get(subAppPath);
                Long lastHeatBeatTime = 0l;
                try {
                    lastHeatBeatTime = Long.parseLong(subApp.get(JstormKeys.APP_HEARTBEAT_TIME));
                } catch (Exception e) {
                    LOG.error(e);
                }
                LOG.info("now is:" + now.getTime());
                LOG.info("lastBeat is:" + lastHeatBeatTime);
                if (now.getTime() - lastHeatBeatTime > 5 * JstormKeys.HEARTBEAT_TIME_INTERVAL
                        || lastHeatBeatTime > now.getTime() || subAppPath.trim().equals(appPath.trim())) {
                    registryOperations.delete(subAppPath, true);
                }
//                else {
//                    done = true;
//                    LOG.info("This instance already has a live application:" + subAppPath + "  lastHeartBeat:" + lastHeatBeatTime);
//                    LOG.info("application quit");
//                }
            }
        }

        if (!done) {
            jstormMasterContext = new JstormMasterContext(user, null, appAttemptID, 0, appMasterHostname, conf);

            registryOperations.mknode(appPath, true);
            registryOperations.bind(instancePath, application, BindFlags.OVERWRITE);

            ServiceRecord previousRegister = registryOperations.resolve(instancePath);
            LOG.info("previousRegister:" + previousRegister.toString());

            LOG.info("register path: " + instancePath);
            AMServer as = new AMServer(appMasterThriftPort);
            as.Start(this);
        }

//        for (int i = 0; i < numTotalContainersToRequest; ++i) {
//            ContainerRequest containerAsk = setupContainerAskForRM(4000, 4, 1, "*");
//            amRMClient.addContainerRequest(containerAsk);
//        }
    }

    private ServiceRecord setupServiceRecord() {
        ServiceRecord application = new ServiceRecord();
//        application.set(YarnRegistryAttributes.YARN_ID, RegistryPathUtils.encodeYarnID(appAttemptID.getApplicationId().toString()));
        application.set(YarnRegistryAttributes.YARN_ID, appAttemptID.getApplicationId().toString());
        application.description = "am";
        application.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                PersistencePolicies.PERMANENT);

//        InetAddress addr = InetAddress.getLocalHost();
        Map<String, String> addresses = new HashMap<String, String>();
        addresses.put("host", appMasterHostname);
        addresses.put("port", String.valueOf(appMasterThriftPort));

        Endpoint endpoint = new Endpoint("http", "host/port", "rpc", addresses);
        application.addExternalEndpoint(endpoint);
        return application;
    }

    @VisibleForTesting
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

    @VisibleForTesting
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    @VisibleForTesting
    protected boolean finish() {
        // wait for completion.
        String appPath;
        while (!done
                ) {
            try {
                Thread.sleep(JstormKeys.HEARTBEAT_TIME_INTERVAL);
                appPath = RegistryUtils.servicePath(
                        JstormKeys.APP_TYPE, instanceName, appAttemptID.getApplicationId().toString());
                ServiceRecord app = new ServiceRecord();
                Date now = new Date();
                app.set(JstormKeys.APP_HEARTBEAT_TIME, String.valueOf(now.getTime()));
                registryOperations.bind(appPath, app, BindFlags.OVERWRITE);

            } catch (Exception ex) {
                LOG.error(ex);
            }
        }

        if (timelineClient != null) {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                    DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
        }

        appPath = RegistryUtils.servicePath(
                JstormKeys.APP_TYPE, instanceName, appAttemptID.getApplicationId().toString());
        try {
            registryOperations.delete(appPath, true);
            LOG.info("unRegister application' appPath:" + appPath);
        } catch (IOException e) {
            LOG.error("Failed to unRegister application's Registry", e);
        }

        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: ", e);
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
        if (numFailedContainers.get() == 0 &&
                numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get() + ", allocated="
                    + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
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
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                Map<Long, Container> nimbusMap = new HashMap<Long, Container>();
                for (Container container : nimbusContainers) {
                    nimbusMap.put(container.getId().getContainerId(), container);
                }
                Map<Long, Container> supervisorMap = new HashMap<Long, Container>();
                for (Container container : supervisorContainers) {
                    supervisorMap.put(container.getId().getContainerId(), container);
                }

                Long containerId = containerStatus.getContainerId().getContainerId();

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }

                    if (nimbusMap.containsKey(containerId)) {
                        nimbusContainers.remove(nimbusMap.get(containerId));
                    } else if (supervisorMap.containsKey(containerId)) {
                        supervisorContainers.remove(supervisorMap.get(containerId));
                    }

                } else {
                    //if container over and wasn't killed by framework ,then resend ContainerRequest and launch it again
                    numCompletedContainers.incrementAndGet();
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
                            requestBlockingQueue.put(containerAsk);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                String containerPath = RegistryUtils.componentPath(
                        JstormKeys.APP_TYPE, instanceName, appAttemptID.getApplicationId().toString(), containerStatus.getContainerId().toString());
                try {
                    if (registryOperations.exists(containerPath)) {
                        registryOperations.delete(containerPath, true);
                    }
                } catch (Exception ex) {
                    LOG.error("error to delete registry of container when container complete", ex);
                }
                if (timelineClient != null) {
                    publishContainerEndEvent(
                            timelineClient, containerStatus, domainId, appSubmitterUgi);
                }
            }

            // ask for more containers if any failed
            // we can do HA in this place at container level

//            int askCount = numTotalContainers - numRequestedContainers.get();
//            numRequestedContainers.addAndGet(askCount);
//
//            if (askCount > 0) {
//                for (int i = 0; i < askCount; ++i) {
//                    ContainerRequest containerAsk = setupContainerAskForRM();
//                    amRMClient.addContainerRequest(containerAsk);
//                }
//            }

//            if (numCompletedContainers.get() == numTotalContainers) {
//                done = true;
//            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
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
                // + ", containerToken"
                // +allocatedContainer.getContainerToken().getIdentifier().toString());


                // check priority to  assign start type . this priority was assigned by JstormAMHandler
                STARTType startType;
                //todo: register every supervisor containers host
                if (allocatedContainer.getPriority().getPriority() == 0) {

                    String supervisorHost = allocatedContainer.getNodeId().getHost();
                    startType = STARTType.SUPERVISOR;
                    String containerPath = RegistryUtils.componentPath(
                            JstormKeys.APP_TYPE, instanceName,
                            allocatedContainer.getId().getApplicationAttemptId().getApplicationId().toString(), allocatedContainer.getId().toString());

                    ServiceRecord sr = null;

                    try {
                        if (!registryOperations.exists(containerPath)) {
                            registryOperations.mknode(containerPath, true);
                            sr = new ServiceRecord();
                            sr.set("host", supervisorHost);
                            sr.set(YarnRegistryAttributes.YARN_ID, allocatedContainer.getId().toString());
                            sr.description = "container";
                            sr.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                                    PersistencePolicies.CONTAINER);
                            registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                        }
//                        sr = registryOperations.resolve(containerPath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    startType = STARTType.NIMBUS;
                    // set nimbusHost
                    nimbusHost = allocatedContainer.getNodeId().getHost();
//                    String path = RegistryUtils.servicePath(
//                            JstormKeys.APP_TYPE, instanceName, appAttemptID.getApplicationId().toString());
                    String path = RegistryUtils.serviceclassPath(
                            JstormKeys.APP_TYPE, instanceName);

                    // when nimbus restart or failed,we need reload nimbus data in previous nimbus container
                    // so when nimbus container allocated we register nimbus's host, directory and containerId,  pull previous nimbus
                    // data from previous nimbus host if necessary.
                    ServiceRecord serviceRecord = setupServiceRecord();
                    previousNimbusHost = "";
                    String previousNimbusLocalDir = "";
                    String previousNimbusContainerId = "";
                    try {
                        ServiceRecord sr = registryOperations.resolve(path);
                        previousNimbusHost = sr.get("nimbus.host", "");
                        previousNimbusLocalDir = sr.get("nimbus.localdir", "");
                        previousNimbusContainerId = sr.get("nimbus.containerId", "");


                        LOG.info("previousNimbusHost is :" + previousNimbusHost);
                        LOG.info("nimbusHost is :" + nimbusHost);

                        //  nimbus location register, then we can restart nimbus with no work loss
                        serviceRecord.set("nimbus.host", nimbusHost);
                        serviceRecord.set("nimbus.localdir", nimbusDataDirPrefix);
                        serviceRecord.set("nimbus.containerId", allocatedContainer.getId().toString());
                        registryOperations.bind(path, serviceRecord, BindFlags.OVERWRITE);

                    } catch (Exception ex) {
                        LOG.error(ex);
                    }
                    LOG.info("allocated nimbus container , nimbus host is :" + nimbusHost);
                }

                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener, startType);
                Thread launchThread = new Thread(runnableLaunchContainer);
                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();

                RefreshNodesRunnable refreshNodesThread = new RefreshNodesRunnable();
                launchThreads.add(refreshNodesThread);
                refreshNodesThread.start();

                // need to remove container request when allocated,
                // otherwise RM will continues allocate container over needs
                if (!requestBlockingQueue.isEmpty()) {
                    try {
                        amRMClient.removeContainerRequest(requestBlockingQueue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
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
            done = true;
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
                        applicationMaster.domainId, applicationMaster.appSubmitterUgi);
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
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

    private class RefreshNodesRunnable extends Thread {

        @Override
        public void run() {
            try {
                List<NodeReport> newReports = yarnClient.getNodeReports(NodeState.RUNNING);
                synchronized (JstormMaster.this.nodeReports) {
                    JstormMaster.this.nodeReports.clear();
                    JstormMaster.this.nodeReports.addAll(newReports);
                    JstormMaster.this.lastNodesRefreshTime = System.currentTimeMillis();
                }
            } catch (YarnException | IOException ex) {
                LOG.error("Failed to get node reports, will retry later...", ex);
            }

            try {
                Thread.sleep(30 * 1000);
            } catch (Exception ignored) {
            }
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

            // The container for the eventual shell commands needs its own local
            // resources too.
            // In this scenario, if a shell script is specified, we need to have it
            // copied and made available to the container.
            if (!scriptPath.isEmpty()) {
                Path renamedScriptPath;
                if (Shell.WINDOWS) {
                    renamedScriptPath = new Path(scriptPath + ".bat");
                } else {
                    renamedScriptPath = new Path(scriptPath + ".sh");
                }

                try {
                    // rename the script file based on the underlying OS syntax.
                    renameScriptFile(renamedScriptPath);
                } catch (Exception e) {
                    LOG.error(
                            "Not able to add suffix (.bat/.sh) to the shell script filename",
                            e);
                    // We know we cannot continue launching the container
                    // so we should release it.
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
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
                    jarUrl = ConverterUtils.getYarnUrlFromURI(new URI(appMasterJarPath));
                } catch (URISyntaxException e) {
                    LOG.error("Error when trying to use shell script path specified"
                            + " in env, path=" + appMasterJarPath, e);
                    return;
                }
                try {
                    //Configuration.addDefaultResource("hdfs-site.xml");
                    //Configuration.addDefaultResource("core-site.xml");

                    FileSystem fileSystem = FileSystem.get(conf);
                    FileStatus appMasterJarPathStatus = fileSystem.getFileStatus(new Path(appMasterJarPath));
                    jarPathLen = appMasterJarPathStatus.getLen();
                    jarTimestamp = appMasterJarPathStatus.getModificationTime();
                    FileStatus scriptStatus = fileSystem.getFileStatus(renamedScriptPath);
                    shellScriptPathLen = scriptStatus.getLen();
                    shellScriptPathTimestamp = scriptStatus.getModificationTime();

                    LOG.info("jarPathLen:" + jarPathLen + " jarTimepstamp:" + jarTimestamp);

                } catch (IOException e) {
                    LOG.error("get hdfs filestatus"
                            + " in env, path=" + appMasterJarPath, e);
                }

                LocalResource shellRsrc = LocalResource.newInstance(yarnUrl,
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        shellScriptPathLen, shellScriptPathTimestamp);
                localResources.put(ExecShellStringPath, shellRsrc);

                LocalResource jarRsrc = LocalResource.newInstance(jarUrl,
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        jarPathLen, jarTimestamp);
                localResources.put("AppMaster.jar", jarRsrc);

                LOG.info(shellRsrc.getResource().getFile());
                LOG.info(jarRsrc.getResource().getFile());

                shellCommand = Shell.WINDOWS ? windows_command : linux_bash_command;
            }

            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(9);

            // Set executable command
            vargs.add(shellCommand);
            // Set shell script path
            if (!scriptPath.isEmpty()) {
                vargs.add(Shell.WINDOWS ? ExecBatScripStringtPath
                        : ExecShellStringPath);
            }

            String startTypeStr = "supervisor";
            // start type specified to be excute by shell script to start jstorm process
            if (startType == STARTType.NIMBUS) {
                startTypeStr = "nimbus";
                vargs.add("nimbus");
                //put containerId in nimbus containers queue
                try {
                    nimbusContainers.put(this.container);
                } catch (InterruptedException ignored) {
                }
            } else {
                vargs.add("supervisor");
                try {
                    supervisorContainers.put(this.container);
                } catch (InterruptedException ignored) {
                }
            }

            // pass instanceName for multiple instance deploy
            nimbusDataDirPrefix = conf.get("jstorm.yarn.instance.dataDir");
//            vargs.add(nimbusDataDirPrefix + instanceName);
            String localDir = nimbusDataDirPrefix + container.getId().toString() + "/" + instanceName;
            vargs.add(localDir);

            // pass jstorm deploy file path on hdfs for pull that down
//            deployPath = conf.get("jstorm.yarn.instance.deploy.dir");
            vargs.add(deployPath);

            //get superviorhost's free port
            SlotPortsView slotPortsView = new SlotPortsView(instanceName, container.getId(), registryOperations);
            slotPortsView.setMinPort(conf.getInt("jstorm.yarn.supervisor.minport", JOYConstants.PORT_RANGE_MIN));
            slotPortsView.setMaxPort(conf.getInt("jstorm.yarn.supervisor.maxport", JOYConstants.PORT_RANGE_MAX));
            String slotPortsStr = "";
            try {
                slotPortsStr = slotPortsView.getSupervisorSlotPorts(container.getResource().getMemory(),
                        container.getResource().getVirtualCores(), container.getNodeId().getHost());

                vargs.add(slotPortsStr);
            } catch (Exception ex) {
                LOG.error("failed get slot ports , container " + container.toString() + "launch fail", ex);
                return;
            }
            String logviewPort = "8622";
            String nimbusThriftPort = "8627";
            try {
                logviewPort = slotPortsView.getSupervisorSlotPorts(4110,
                        1, container.getNodeId().getHost());
                nimbusThriftPort = slotPortsView.getSupervisorSlotPorts(4110,
                        1, container.getNodeId().getHost());
//                supervisorLogviewPort = slotPortsView.getSetPortUsedBySupervisor(container.getNodeId().getHost(), 1).get(0);
            } catch (Exception e) {
                e.printStackTrace();
            }
//            int slotCount = JstormYarnUtils.getSlotCount(container.getResource().getMemory(),
//                    container.getResource().getVirtualCores());
//            List<String> virtualPorts = new ArrayList<String>();
//            for (int i = 0; i < slotCount; i++) {
//                virtualPorts.add(String.valueOf(i));
//            }
//            String virtualPortsStr = JstormYarnUtils.join(virtualPorts, ",", false);
//            vargs.add(virtualPortsStr);

            String hadoopHome = conf.get("jstorm.yarn.hadoop.home");
            String javaHome = conf.get("jstorm.yarn.java.home");
            String pythonHome = conf.get("jstorm.yarn.python.home");
            vargs.add(hadoopHome);
            vargs.add(javaHome);//$6
            vargs.add(pythonHome);//$7

            String deployDst = conf.get("jstorm.yarn.instance.deploy.destination");
            if (deployDst == null) {
                deployDst = nimbusDataDirPrefix;
            }
            String dstPath = deployDst + container.getId().toString();
            vargs.add(dstPath);//$8

            //now cgroup is not supported yet, so disable slot ports
//            vargs.add("");

            // Set args for the shell command if any
            vargs.add(shellArgs);
            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
//            commands.add(command.toString());
//            LOG.info("container command is :" + command.toString());

            Map<String, String> envs = System.getenv();
            String exectorCommand = ExecutorLoader.loadCommand(instanceName, shellCommand, startTypeStr,
                    this.container.getId().toString(), localDir, deployPath, hadoopHome, javaHome, pythonHome, dstPath, slotPortsStr, shellArgs,
                    envs.get("CLASSPATH"), ExecShellStringPath, appAttemptID.getApplicationId().toString(), logviewPort, nimbusThriftPort);

//            exectorCommand = exectorCommand + " 1>" + "/home/jian.feng" + "/stdout"
//                    + " 2>" + "/home/jian.feng" + "/stderr";
            exectorCommand = exectorCommand + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                    + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

            commands.add(exectorCommand);

            LOG.info("a container command is :" + exectorCommand);

            // Set up ContainerLaunchContext, setting local resource, environment,
            // command and token for constructor.

            // Note for tokens: Set up tokens for the container too. Today, for normal
            // shell commands, the container in distribute-shell doesn't need any
            // tokens. We are populating them mainly for NodeManagers to be able to
            // download anyfiles in the distributed file-system. The tokens are
            // otherwise also useful in cases, for e.g., when one is running a
            // "hadoop dfs" command inside the distributed shell.
//            shellEnv.put(JOYConstants.CONTAINER_SUPERVISOR_HEARTBEAT, localDir + "/");
//            shellEnv.put(JOYConstants.CONTAINER_NIMBUS_HEARTBEAT, localDir + "/");
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, shellEnv, commands, null, allTokens.duplicate(), null);
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
                fs.rename(new Path(scriptPath), renamedScriptPath);
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

    private boolean fileExist(String filePath) {
        return new File(filePath).exists();
    }

    private String readContent(String filePath) throws IOException {
        DataInputStream ds = null;
        try {
            ds = new DataInputStream(new FileInputStream(filePath));
            return ds.readUTF();
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(ds);
        }
    }

    private static void publishContainerStartEvent(
            final TimelineClient timelineClient, Container container, String domainId,
            UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_START.toString());
        event.addEventInfo("Node", container.getNodeId().toString());
        event.addEventInfo("Resources", container.getResource().toString());
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
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_END.toString());
        event.addEventInfo("State", container.getState().name());
        event.addEventInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (YarnException | IOException e) {
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
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (YarnException | IOException e) {
            LOG.error("App Attempt "
                    + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
                    + " event could not be published for "
                    + appAttemptId, e);
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
            rmRegOperations.initUserRegistryAsync(service_user_name);
        }
    }

    /**
     * Handler for @link RegisterComponentInstance action}
     * Register/re-register an ephemeral container that is already in the app state
     *
     * @param id          the component
     * @param description component description
     * @return true if the component is registered
     */
    public boolean registerComponent(ContainerId id, String description) throws
            IOException {

        // this is where component registrations  go
        LOG.info("Registering component {}" + id);
        String cid = RegistryPathUtils.encodeYarnID(id.toString());
        ServiceRecord container = new ServiceRecord();
        container.set(YarnRegistryAttributes.YARN_ID, cid);
        container.description = description;
        container.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                PersistencePolicies.CONTAINER);
        try {
            String path = RegistryUtils.componentPath(
                    RegistryUtils.currentUser(), JstormKeys.APP_TYPE,
                    "instanceName", cid);
            registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
            registryOperations.bind(path, container, BindFlags.OVERWRITE);
//            portScanner.getAvailablePort();
//            InetSocketAddress rpcAddress = new InetSocketAddress("0.0.0.0", port);
//            NetUtils.getConnectAddress(server);

//            yarnRegistryOperations.putComponent(cid, container);
        } catch (IOException e) {
            LOG.warn("Failed to register container" + id + " /" + description + ": " + e + ""
            );
            return false;
        }
        return true;
    }

    /**
     * Handler for  UnregisterComponentInstance}
     * <p/>
     * unregister a component. At the time this message is received,
     * the component may not have been registered
     *
     * @param id the component
     */
    public void unregisterComponent(ContainerId id) {
        LOG.info("Unregistering component " + id);

        String cid = RegistryPathUtils.encodeYarnID(id.toString());
        try {
            String path = RegistryUtils.componentPath(
                    RegistryUtils.currentUser(), JstormKeys.APP_TYPE,
                    "instanceName",
                    cid);
            registryOperations.delete(path, false);
//            yarnRegistryOperations.deleteComponent(cid);
        } catch (IOException e) {
            LOG.warn("Failed to delete container " + id + " : " + e);
        }
    }
}