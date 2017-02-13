package com.alibaba.jstorm.yarn;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.context.JstormClientContext;
import com.alibaba.jstorm.yarn.utils.JstormYarnUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * Created by fengjian on 15/12/22.
 * application client
 */
public class JstormOnYarn {
    private static final Log LOG = LogFactory.getLog(JstormOnYarn.class);
    private final String appMasterMainClass;
    private JstormClientContext jstormClientContext = new JstormClientContext();

    public JstormOnYarn() throws Exception {
        this(new YarnConfiguration());
    }

    public JstormOnYarn(Configuration conf) throws Exception {
        this(JOYConstants.MASTER_CLASS_NAME, conf);
    }

    JstormOnYarn(String appMasterMainClass, Configuration conf) {
        this.jstormClientContext.conf = conf;
        this.appMasterMainClass = appMasterMainClass;
        jstormClientContext.yarnClient = YarnClient.createYarnClient();
        jstormClientContext.yarnClient.init(conf);
        jstormClientContext.opts = JstormYarnUtils.initClientOptions();
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp(JOYConstants.CLIENT, jstormClientContext.opts);
    }

    /**
     * Parse command line options
     */
    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(jstormClientContext.opts, args);
        if (cliParser.hasOption(JOYConstants.HELP)) {
            printUsage();
            return false;
        }
        jstormClientContext.appName = cliParser.getOptionValue(JOYConstants.APP_NAME_KEY, JOYConstants.CLIIENT_CLASS);
        jstormClientContext.amPriority = Integer.parseInt(cliParser.getOptionValue(JOYConstants.PRIORITY, JOYConstants.DEFAULT_PRIORITY));
        jstormClientContext.amQueue = cliParser.getOptionValue(JOYConstants.QUEUE, JOYConstants.QUEUE_NAME);
        jstormClientContext.amMemory = Integer.parseInt(cliParser.getOptionValue(JOYConstants.MASTER_MEMORY, JOYConstants.DEFAULT_MASTER_MEMORY));
        jstormClientContext.amVCores = Integer.parseInt(cliParser.getOptionValue(JOYConstants.MASTER_VCORES, JOYConstants.DEFAULT_MASTER_VCORES));

        jstormClientContext.appMasterJar = cliParser.getOptionValue(JOYConstants.JAR);
        jstormClientContext.libJars = cliParser.getOptionValue(JOYConstants.LIB_JAR);
        jstormClientContext.homeDir = cliParser.getOptionValue(JOYConstants.HOME_DIR);
        jstormClientContext.confFile = cliParser.getOptionValue(JOYConstants.CONF_FILE);
        jstormClientContext.rmHost = cliParser.getOptionValue(JOYConstants.RM_ADDRESS, JOYConstants.EMPTY);
        jstormClientContext.nameNodeHost = cliParser.getOptionValue(JOYConstants.NN_ADDRESS, JOYConstants.EMPTY);
        jstormClientContext.deployPath = cliParser.getOptionValue(JOYConstants.DEPLOY_PATH, JOYConstants.EMPTY);
        jstormClientContext.hadoopConfDir = cliParser.getOptionValue(JOYConstants.HADOOP_CONF_DIR, JOYConstants.EMPTY);
        jstormClientContext.instanceName = cliParser.getOptionValue(JOYConstants.INSTANCE_NAME, JOYConstants.EMPTY);

        JstormYarnUtils.checkAndSetOptions(cliParser, jstormClientContext);
        return true;
    }

    /**
     * Main run function for the client
     *
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {

        LOG.info("Running Client");
        jstormClientContext.yarnClient.start();

        YarnClusterMetrics clusterMetrics = jstormClientContext.yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = jstormClientContext.yarnClient.getNodeReports(
                NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM, count is " + String.valueOf(clusterNodeReports.size()));

        QueueInfo queueInfo = jstormClientContext.yarnClient.getQueueInfo(this.jstormClientContext.amQueue);
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = jstormClientContext.yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }

        if (jstormClientContext.domainId != null && jstormClientContext.domainId.length() > 0 && jstormClientContext.toCreateDomain) {
            prepareTimelineDomain();
        }

        // Get a new application id
        YarnClientApplication app = jstormClientContext.yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (jstormClientContext.amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + jstormClientContext.amMemory
                    + ", max=" + maxMem);
            jstormClientContext.amMemory = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (jstormClientContext.amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + jstormClientContext.amVCores
                    + ", max=" + maxVCores);
            jstormClientContext.amVCores = maxVCores;
        }

        // set the application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(jstormClientContext.keepContainers);
        if (!StringUtils.isBlank(jstormClientContext.instanceName) && JOYConstants.CLIIENT_CLASS.equals(jstormClientContext.appName)) {
            appContext.setApplicationName(jstormClientContext.appName + JOYConstants.DOT + jstormClientContext.instanceName);
        } else {
            appContext.setApplicationName(jstormClientContext.appName);
        }


        if (jstormClientContext.attemptFailuresValidityInterval >= 0) {
            appContext.setAttemptFailuresValidityInterval(jstormClientContext.attemptFailuresValidityInterval);
        }

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        FileSystem fs = FileSystem.get(jstormClientContext.conf);
        addToLocalResources(fs, jstormClientContext.appMasterJar, JOYConstants.appMasterJarPath, appId.toString(),
                localResources, null);

        //add jstormOnYarn's configuration to resources which path is jstorm-yarn.xml
        if (jstormClientContext.confFile == null || jstormClientContext.confFile.isEmpty()) {
            addToLocalResources(fs, JOYConstants.CONF_NAME, JOYConstants.CONF_NAME, appId.toString(),
                    localResources, null);
        } else {
            addToLocalResources(fs, jstormClientContext.confFile, JOYConstants.CONF_NAME, appId.toString(),
                    localResources, null);
        }

        if (jstormClientContext.libJars != null && !jstormClientContext.libJars.isEmpty()) {
            for (String libPath : jstormClientContext.libJars.split(JOYConstants.COMMA)) {
                String[] strArr = libPath.split(JOYConstants.BACKLASH);
                String libName = strArr[strArr.length - 1];
                addToLocalResources(fs, libPath, libName, appId.toString(),
                        localResources, null);
            }
        }

        // Set the log4j properties if needed
        if (!jstormClientContext.log4jPropFile.isEmpty()) {
            addToLocalResources(fs, jstormClientContext.log4jPropFile, JOYConstants.log4jPath, appId.toString(),
                    localResources, null);
        }

        // The shell script has to be made available on the final container(s)
        // where it will be executed.
        String hdfsShellScriptLocation = JOYConstants.EMPTY;
        long hdfsShellScriptLen = 0;
        long hdfsShellScriptTimestamp = 0;
        if (!jstormClientContext.shellScriptPath.isEmpty()) {
            Path shellSrc = new Path(jstormClientContext.shellScriptPath);
            String shellPathSuffix =
                    jstormClientContext.appName + JOYConstants.BACKLASH + appId.toString() + JOYConstants.BACKLASH + JOYConstants.SCRIPT_PATH;
            Path shellDst =
                    new Path(fs.getHomeDirectory(), shellPathSuffix);
            fs.copyFromLocalFile(false, true, shellSrc, shellDst);
            hdfsShellScriptLocation = shellDst.toUri().toString();
            FileStatus shellFileStatus = fs.getFileStatus(shellDst);
            hdfsShellScriptLen = shellFileStatus.getLen();
            hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
        }

        if (!jstormClientContext.shellCommand.isEmpty()) {
            addToLocalResources(fs, null, JOYConstants.shellCommandPath, appId.toString(),
                    localResources, jstormClientContext.shellCommand);
        }

        if (jstormClientContext.shellArgs.length > 0) {
            addToLocalResources(fs, null, JOYConstants.shellArgsPath, appId.toString(),
                    localResources, StringUtils.join(jstormClientContext.shellArgs, JOYConstants.BLANK));
        }

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        Path appMaterJar =
                new Path(fs.getHomeDirectory(), jstormClientContext.appName + JOYConstants.BACKLASH + appId.toString() + JOYConstants.BACKLASH + JOYConstants.appMasterJarPath);
        FileStatus jarFIleStatus = fs.getFileStatus(appMaterJar);

        // put location of shell script into env
        // using the env info, the application master will create the correct local resource for the
        // eventual containers that will be launched to execute the shell scripts
        env.put(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, hdfsShellScriptLocation);
        env.put(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, hdfsShellScriptLocation);
        env.put(JOYConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP, Long.toString(hdfsShellScriptTimestamp));
        env.put(JOYConstants.DISTRIBUTEDSHELLSCRIPTLEN, Long.toString(hdfsShellScriptLen));
        if (jstormClientContext.domainId != null && jstormClientContext.domainId.length() > 0) {
            env.put(JOYConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN, jstormClientContext.domainId);
        }

        env.put(JOYConstants.APPMASTERJARSCRIPTLOCATION, appMaterJar.toUri().toString());
        env.put(JOYConstants.APPMASTERLEN, Long.toString(jarFIleStatus.getLen()));
        env.put(JOYConstants.APPMASTERTIMESTAMP, Long.toString(jarFIleStatus.getModificationTime()));
        env.put(JOYConstants.APPMASTERJARSCRIPTLOCATION, appMaterJar.toUri().toString());

        env.put(JOYConstants.BINARYFILEDEPLOYPATH, jstormClientContext.deployPath);
        env.put(JOYConstants.INSTANCENAME, jstormClientContext.instanceName);

        // Add AppMaster.jar location to classpath
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(JOYConstants.DOT + JOYConstants.BACKLASH + JOYConstants.ASTERISK);
        for (String c : jstormClientContext.conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                JOYConstants.DOT + JOYConstants.BACKLASH + JOYConstants.LOG_PROPERTIES);

        // add the runtime classpath needed for tests to work
        if (jstormClientContext.conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(JOYConstants.COLON);
            classPathEnv.append(System.getProperty(JOYConstants.JAVA_CLASS_PATH));
        }

        env.put(JOYConstants.CLASS_PATH, classPathEnv.toString());
        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        // Set java executable command
        vargs.add(Environment.JAVA_HOME.$$() + JOYConstants.JAVA);
        // Set Xmx based on am memory size
        vargs.add(JOYConstants.XMX + jstormClientContext.amMemory + JOYConstants.MB);
        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add(JOYConstants.CLI_PREFIX + JOYConstants.CONTAINER_MEMORY + JOYConstants.BLANK + String.valueOf(jstormClientContext.containerMemory));
        vargs.add(JOYConstants.CLI_PREFIX + JOYConstants.CONTAINER_VCORES + JOYConstants.BLANK + String.valueOf(jstormClientContext.containerVirtualCores));
        vargs.add(JOYConstants.CLI_PREFIX + JOYConstants.NUM_CONTAINERS + JOYConstants.BLANK + String.valueOf(jstormClientContext.numContainers));
        if (null != jstormClientContext.nodeLabelExpression) {
            appContext.setNodeLabelExpression(jstormClientContext.nodeLabelExpression);
        }
        vargs.add(JOYConstants.CLI_PREFIX + JOYConstants.PRIORITY + JOYConstants.BLANK + String.valueOf(jstormClientContext.shellCmdPriority));

        for (Map.Entry<String, String> entry : jstormClientContext.shellEnv.entrySet()) {
            vargs.add(JOYConstants.CLI_PREFIX + JOYConstants.SHELL_ENV + JOYConstants.BLANK + entry.getKey() + JOYConstants.EQUAL + entry.getValue());
        }
        if (jstormClientContext.debugFlag) {
            vargs.add(JOYConstants.CLI_PREFIX + JOYConstants.DEBUG);
        }

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + JOYConstants.APPMASTER_STDOUT);
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + JOYConstants.APPMASTER_STDERR);

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(JOYConstants.BLANK);
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        // Set up resource type requirements
        Resource capability = Resource.newInstance(jstormClientContext.amMemory, jstormClientContext.amVCores);
        appContext.setResource(capability);

        // Service data is a binary blob that can be passed to the application
        // Not needed in this scenario
        // amContainer.setServiceData(serviceData);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = jstormClientContext.conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }
            // For now, only getting tokens for the default file-system.
            final Token<?> tokens[] =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Priority.newInstance(jstormClientContext.amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(jstormClientContext.amQueue);

        LOG.info("Submitting application to ASM");

        //check configuration
        if (JstormYarnUtils.isUnset(jstormClientContext.conf.get(JOYConstants.INSTANCE_NAME_KEY))) {
            throw new IOException(JOYConstants.INSTANCE_NAME_KEY + " is not set");
        }
        if (JstormYarnUtils.isUnset(jstormClientContext.conf.get(JOYConstants.INSTANCE_DEPLOY_DIR_KEY))) {
            throw new IOException(JOYConstants.INSTANCE_DEPLOY_DIR_KEY + " is not set");
        }
        jstormClientContext.yarnClient.submitApplication(appContext);

        // Monitor the application
        return monitorApplication(appId);
    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     *
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        Integer monitorTimes = JOYConstants.MONITOR_TIMES;

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(JOYConstants.MONITOR_TIME_INTERVAL);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }
            // Get application report for the appId we are interested in
            ApplicationReport report = jstormClientContext.yarnClient.getApplicationReport(appId);

            try {
                File writename = new File(JOYConstants.RPC_ADDRESS_FILE);
                writename.createNewFile();
                BufferedWriter out = new BufferedWriter(new FileWriter(writename));
                out.write(report.getHost() + JOYConstants.NEW_LINE);
                out.write(report.getRpcPort() + JOYConstants.NEW_LINE);
                out.flush();
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            } else if (YarnApplicationState.RUNNING == state) {
                LOG.info("Application is running successfully. Breaking monitoring loop");
                return true;
            } else {
                if (monitorTimes-- <= 0) {
                    forceKillApplication(appId);
                    return false;
                }
            }
        }
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        jstormClientContext.yarnClient.killApplication(appId);
    }

    private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = jstormClientContext.appName + JOYConstants.BACKLASH + appId + JOYConstants.BACKLASH + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission(JOYConstants.FS_PERMISSION));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    private void prepareTimelineDomain() {
        TimelineClient timelineClient = null;
        if (jstormClientContext.conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(jstormClientContext.conf);
            timelineClient.start();
        } else {
            LOG.warn("Cannot put the domain " + jstormClientContext.domainId +
                    " because the timeline service is not enabled");
            return;
        }
        try {
            TimelineDomain domain = new TimelineDomain();
            domain.setId(jstormClientContext.domainId);
            domain.setReaders(
                    jstormClientContext.viewACLs != null && jstormClientContext.viewACLs.length() > 0 ? jstormClientContext.viewACLs : JOYConstants.BLANK);
            domain.setWriters(
                    jstormClientContext.modifyACLs != null && jstormClientContext.modifyACLs.length() > 0 ? jstormClientContext.modifyACLs : JOYConstants.BLANK);
            timelineClient.putDomain(domain);
            LOG.info("Put the timeline domain: " +
                    TimelineUtils.dumpTimelineRecordtoJSON(domain));
        } catch (Exception e) {
            LOG.error("Error when putting the timeline domain", e);
        } finally {
            timelineClient.stop();
        }
    }

    /**
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            JstormOnYarn client = new JstormOnYarn();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(JOYConstants.EXIT_SUCCESS);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(JOYConstants.EXIT_FAIL);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running Client", t);
            System.exit(JOYConstants.EXIT_FAIL);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(JOYConstants.EXIT_SUCCESS);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(JOYConstants.EXIT_FAIL);
    }

}