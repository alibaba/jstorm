package com.alibaba.jstorm.yarn.utils;

import com.alibaba.jstorm.yarn.JstormOnYarn;
import com.alibaba.jstorm.yarn.Log4jPropertyHelper;
import com.alibaba.jstorm.yarn.appmaster.JstormMaster;
import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.constants.JstormXmlConfKeys;
import com.alibaba.jstorm.yarn.context.JstormClientContext;
import com.alibaba.jstorm.yarn.context.JstormMasterContext;
import com.alibaba.jstorm.yarn.context.MasterContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import static com.alibaba.jstorm.yarn.constants.JOYConstants.log4jPath;
import static com.alibaba.jstorm.yarn.constants.JOYConstants.shellArgsPath;
import static com.alibaba.jstorm.yarn.constants.JOYConstants.shellCommandPath;

/**
 * Created by fengjian on 15/12/29.
 */
public class JstormYarnUtils {
    private static final Log LOG = LogFactory.getLog(JstormYarnUtils.class);

    /**
     * Implementation of set-ness, groovy definition of true/false for a string
     *
     * @param s string
     * @return true iff the string is neither null nor empty
     */
    public static boolean isUnset(String s) {
        return s == null || s.isEmpty();
    }

    public static boolean isSet(String s) {
        return !isUnset(s);

    }

    public static int findFreePort(int start, int limit) {
        if (start == 0) {
            //bail out if the default is "dont care"
            return 0;
        }
        int found = 0;
        int port = start;
        int finish = start + limit;
        while (found == 0 && port < finish) {
            if (isPortAvailable(port)) {
                found = port;
            } else {
                port++;
            }
        }
        return found;
    }

    /**
     * See if a port is available for listening on by trying to listen
     * on it and seeing if that works or fails.
     *
     * @param port port to listen to
     * @return true if the port was available for listening on
     */
    public static boolean isPortAvailable(int port) {
        try {
            ServerSocket socket = new ServerSocket(port);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * See if a port is available for listening on by trying connect to it
     * and seeing if that works or fails
     *
     * @param host
     * @param port
     * @return
     */
    public static boolean isPortAvailable(String host, int port) {
        try {
            Socket socket = new Socket(host, port);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    public static String stringify(Throwable t) {
        StringWriter sw = new StringWriter();
        sw.append(t.toString()).append('\n');
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    /**
     * Create a configuration with Slider-specific tuning.
     * This is done rather than doing custom configs.
     *
     * @return the config
     */
    public static YarnConfiguration createConfiguration() {
        YarnConfiguration conf = new YarnConfiguration();
        patchConfiguration(conf);
        return conf;
    }

    /**
     * Take an existing conf and patch it for  needs. Useful
     * in Service.init & RunService methods where a shared config is being
     * passed in
     *
     * @param conf configuration
     * @return the patched configuration
     */
    public static Configuration patchConfiguration(Configuration conf) {

        //if the fallback option is NOT set, enable it.
        //if it is explicitly set to anything -leave alone
        if (conf.get(JstormXmlConfKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH) == null) {
            conf.set(JstormXmlConfKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH, "true");
        }
        return conf;
    }

    /**
     * Take a collection, return a list containing the string value of every
     * element in the collection.
     *
     * @param c collection
     * @return a stringified list
     */
    public static List<String> collectionToStringList(Collection c) {
        List<String> l = new ArrayList<String>(c.size());
        for (Object o : c) {
            l.add(o.toString());
        }
        return l;
    }

    /**
     * Join an collection of objects with a separator that appears after every
     * instance in the list -including at the end
     *
     * @param collection collection to call toString() on each element
     * @param separator  separator string
     * @return the joined entries
     */
    public static String join(Collection collection, String separator) {
        return join(collection, separator, true);
    }

    /**
     * Join an collection of objects with a separator that appears after every
     * instance in the list -optionally at the end
     *
     * @param collection collection to call toString() on each element
     * @param separator  separator string
     * @param trailing   add a trailing entry or not
     * @return the joined entries
     */
    public static String join(Collection collection,
                              String separator,
                              boolean trailing) {
        StringBuilder b = new StringBuilder();
        // fast return on empty collection
        if (collection.isEmpty()) {
            return trailing ? separator : "";
        }
        for (Object o : collection) {
            b.append(o);
            b.append(separator);
        }
        int length = separator.length();
        String s = b.toString();
        return (trailing || s.isEmpty()) ?
                s : (b.substring(0, b.length() - length));
    }

    /**
     * Join an array of strings with a separator that appears after every
     * instance in the list -including at the end
     *
     * @param collection strings
     * @param separator  separator string
     * @return the joined entries
     */
    public static String join(String[] collection, String separator) {
        return join(collection, separator, true);


    }

    /**
     * Join an array of strings with a separator that appears after every
     * instance in the list -optionally at the end
     *
     * @param collection strings
     * @param separator  separator string
     * @param trailing   add a trailing entry or not
     * @return the joined entries
     */
    public static String join(String[] collection, String separator,
                              boolean trailing) {
        return join(Arrays.asList(collection), separator, trailing);
    }

    /**
     * Join an array of strings with a separator that appears after every
     * instance in the list -except at the end
     *
     * @param collection strings
     * @param separator  separator string
     * @return the list
     */
    public static String joinWithInnerSeparator(String separator,
                                                Object... collection) {
        StringBuilder b = new StringBuilder();
        boolean first = true;

        for (Object o : collection) {
            if (first) {
                first = false;
            } else {
                b.append(separator);
            }
            b.append(o.toString());
            b.append(separator);
        }
        return b.toString();
    }

    /**
     * this is for jstorm configuration's format
     *
     * @param memory
     * @param vcores
     * @param supervisorHost
     * @return
     */
    public static String getSupervisorSlotPorts(int memory, int vcores, String instanceName, String supervisorHost, RegistryOperations registryOperations) {
        return join(getSupervisorPorts(memory, vcores, instanceName, supervisorHost, registryOperations), JOYConstants.COMMA, false);
    }

    /**
     * see if port is using by supervisor
     * if used return true, if not, add this port to registry and return false
     *
     * @param supervisorHost
     * @param port
     * @param registryOperations
     * @return
     */
    public static boolean getSetPortUsedBySupervisor(String instanceName, String supervisorHost, int port, RegistryOperations registryOperations) {

        String appPath = RegistryUtils.serviceclassPath(
                JOYConstants.APP_NAME, JOYConstants.APP_TYPE);
        String path = RegistryUtils.servicePath(
                JOYConstants.APP_NAME, JOYConstants.APP_TYPE, instanceName);
        String hostPath = RegistryUtils.componentPath(
                JOYConstants.APP_NAME, JOYConstants.APP_TYPE, instanceName, supervisorHost);

        try {
            List<String> instanceNames = registryOperations.list(appPath);
            for (String instance : instanceNames) {
                String servicePath = RegistryUtils.servicePath(
                        JOYConstants.APP_NAME, JOYConstants.APP_TYPE, instance);
                Map<String, ServiceRecord> hosts = RegistryUtils.listServiceRecords(registryOperations, servicePath);
                for (String host : hosts.keySet()) {
                    ServiceRecord sr = hosts.get(JOYConstants.HOST);
                    String[] portList = sr.get(JOYConstants.PORT_LIST).split(JOYConstants.COMMA);
                    for (String usedport : portList) {
                        if (Integer.parseInt(usedport) == port)
                            return true;
                    }
                }
            }

            if (registryOperations.exists(path)) {
                ServiceRecord sr = registryOperations.resolve(path);
                String[] portList = sr.get(JOYConstants.PORT_LIST).split(JOYConstants.COMMA);

                String portListUpdate = join(portList, JOYConstants.COMMA, true) + String.valueOf(port);
                sr.set(JOYConstants.PORT_LIST, portListUpdate);
                registryOperations.bind(path, sr, BindFlags.OVERWRITE);
                return false;
            } else {
                registryOperations.mknode(path, true);
                ServiceRecord sr = new ServiceRecord();
                String portListUpdate = String.valueOf(port);
                sr.set(JOYConstants.PORT_LIST, portListUpdate);
                registryOperations.bind(path, sr, BindFlags.OVERWRITE);
                return false;
            }
        } catch (Exception ex) {
            return true;
        }
    }

    public static List<String> getSupervisorPorts(int memory, int vcores, String instanceName, String supervisorHost, RegistryOperations registryOperations) {
        List<String> relist = new ArrayList<String>();
        int slotCount = getSlotCount(memory, vcores);
        for (int i = 9000; i < 15000; i++) {
            if (isPortAvailable(supervisorHost, i)) {
                if (!getSetPortUsedBySupervisor(instanceName, supervisorHost, i, registryOperations))
                    relist.add(String.valueOf(i));
            }
            if (relist.size() >= slotCount) {
                break;
            }
        }
        return relist;
    }

    public static int getSlotCount(int memory, int vcores) {
        int cpuports = (int) Math.ceil(vcores / 1.2);
        int memoryports = (int) Math.ceil(memory / 2000.0);
//        return cpuports > memoryports ? memoryports : cpuports;
        //  doesn't support cgroup yet
        return memoryports;
    }

    public static Options initClientOptions() {
        Options opts = new Options();
        opts.addOption(JOYConstants.APP_NAME_KEY, true, "Application Name. Default value - com.alibaba.jstorm.yarn.JstormOnYarn");
        opts.addOption(JOYConstants.PRIORITY, true, "Application Priority. Default 0");
        opts.addOption(JOYConstants.PRIORITY, true, "RM Queue in which this application is to be submitted");
        opts.addOption(JOYConstants.TIMEOUT, true, "Application timeout in milliseconds");
        opts.addOption(JOYConstants.MASTER_MEMORY, true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption(JOYConstants.MASTER_VCORES, true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption(JOYConstants.JAR, true, "Jar file containing the application master");
        opts.addOption(JOYConstants.LIB_JAR, true, "dependency lib");
        opts.addOption(JOYConstants.HOME_DIR, true, "home ");
        opts.addOption(JOYConstants.CONF_FILE, true, " path of jstorm-yarn.xml ");
        opts.addOption(JOYConstants.RM_ADDRESS, true, "resource manager address eg hostname:port");
        opts.addOption(JOYConstants.NN_ADDRESS, true, "nameNode address eg hostname:port");
        opts.addOption(JOYConstants.HADOOP_CONF_DIR, true, "hadoop config directory which contains hdfs-site.xml/core-site" +
                ".xml/yarn-site.xml");
        opts.addOption(JOYConstants.INSTANCE_NAME, true, "instance name , which is path of registry");
        opts.addOption(JOYConstants.DEPLOY_PATH, true, "deploy dir on HDFS  ");
        opts.addOption(JOYConstants.SHELL_SCRIPT, true, "Location of the shell script to be " +
                "executed. Can only specify either --shell_command or --shell_script");
        opts.addOption(JOYConstants.SHELL_ARGS, true, "Command line args for the shell script." +
                "Multiple args can be separated by empty space.");
        opts.getOption(JOYConstants.SHELL_ARGS).setArgs(Option.UNLIMITED_VALUES);
        opts.addOption(JOYConstants.SHELL_ENV, true, "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption(JOYConstants.SHELL_CMD_PRIORITY, true, "Priority for the shell command containers");
        opts.addOption(JOYConstants.CONTAINER_MEMORY, true, "Amount of memory in MB to be requested to run the shell command");
        opts.addOption(JOYConstants.CONTAINER_VCORES, true, "Amount of virtual cores to be requested to run the shell command");
        opts.addOption(JOYConstants.NUM_CONTAINERS, true, "No. of containers on which the shell command needs to be executed");
        opts.addOption(JOYConstants.LOG_PROPERTIES, true, "log4j.properties file");
        opts.addOption(JOYConstants.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS, false,
                "Flag to indicate whether to keep containers across application attempts." +
                        " If the flag is true, running containers will not be killed when" +
                        " application attempt fails and these containers will be retrieved by" +
                        " the new application attempt ");
        opts.addOption(JOYConstants.ATTEMPT_FAILURES_VALIDITY_INTERVAL, true,
                "when attempt_failures_validity_interval in milliseconds is set to > 0," +
                        "the failure number will not take failures which happen out of " +
                        "the validityInterval into failure count. " +
                        "If failure count reaches to maxAppAttempts, " +
                        "the application will be failed.");
        opts.addOption(JOYConstants.DEBUG, false, "Dump out debug information");
        opts.addOption(JOYConstants.DOMAIN, true, "ID of the timeline domain where the "
                + "timeline entities will be put");
        opts.addOption(JOYConstants.VIEW_ACLS, true, "Users and groups that allowed to "
                + "view the timeline entities in the given domain");
        opts.addOption(JOYConstants.MODIFY_ACLS, true, "Users and groups that allowed to "
                + "modify the timeline entities in the given domain");
        opts.addOption(JOYConstants.CREATE, false, "Flag to indicate whether to create the "
                + "domain specified with -domain.");
        opts.addOption(JOYConstants.HELP, false, "Print usage");
        opts.addOption(JOYConstants.NODE_LABEL_EXPRESSION, true,
                "Node label expression to determine the nodes"
                        + " where all the containers of this application"
                        + " will be allocated, \"\" means containers"
                        + " can be allocated anywhere, if you don't specify the option,"
                        + " default node_label_expression of queue will be used.");
        return opts;
    }

    public static void getYarnConfFromJar(String jarPath) {
        String confPath = jarPath + JOYConstants.CONF_NAME;
        try {
            InputStream stream = new FileInputStream(confPath);
            FileOutputStream out = new FileOutputStream(JOYConstants.CONF_NAME);
            byte[] data = IOUtils.toByteArray(stream);
            out.write(data);
            out.close();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "No configuration file specified to be executed by application master to launch process");
        }
    }

    public static void checkAndSetOptions(CommandLine cliParser, JstormClientContext jstormClientContext) {
        if (cliParser.hasOption(JOYConstants.DEBUG)) {
            jstormClientContext.debugFlag = true;
        }
        if (cliParser.hasOption(JOYConstants.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS)) {
            jstormClientContext.keepContainers = true;
        }
        if (cliParser.hasOption(JOYConstants.LOG_PROPERTIES)) {
            String log4jPath = cliParser.getOptionValue(JOYConstants.LOG_PROPERTIES);
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(JstormOnYarn.class, log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }
        if (jstormClientContext.amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + jstormClientContext.amMemory);
        }
        if (jstormClientContext.amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + jstormClientContext.amVCores);
        }
        if (!cliParser.hasOption(JOYConstants.JAR)) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }
        if (!jstormClientContext.rmHost.equals(JOYConstants.EMPTY)) {
            jstormClientContext.conf.set(JOYConstants.RM_ADDRESS_KEY, jstormClientContext.rmHost, JOYConstants.YARN_CONF_MODE);
        }
        if (!jstormClientContext.nameNodeHost.equals(JOYConstants.EMPTY)) {
            jstormClientContext.conf.set(JOYConstants.FS_DEFAULTFS_KEY, jstormClientContext.nameNodeHost);
        }
        if (!StringUtils.isBlank(jstormClientContext.hadoopConfDir)) {
            try {
                Collection<File> files = FileUtils.listFiles(new File(jstormClientContext.hadoopConfDir), new String[]{JOYConstants.XML}, true);
                for (File file : files) {
                    LOG.info("adding hadoop conf file to conf: " + file.getAbsolutePath());
                    jstormClientContext.conf.addResource(file.getAbsolutePath());
                }
            } catch (Exception ex) {
                LOG.error("failed to list hadoop conf dir: " + jstormClientContext.hadoopConfDir);
            }
        }
        String jarPath = JstormOnYarn.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath();
        if (jstormClientContext.confFile == null) {
            JstormYarnUtils.getYarnConfFromJar(jarPath);
            jstormClientContext.conf.addResource(JOYConstants.CONF_NAME);
        } else {
            Path jstormyarnConfPath = new Path(jstormClientContext.confFile);
            jstormClientContext.conf.addResource(jstormyarnConfPath);
        }
        if (!cliParser.hasOption(JOYConstants.SHELL_SCRIPT)) {
            String jarShellScriptPath = jarPath + JOYConstants.START_JSTORM_SHELL;
            try {
                InputStream stream = new FileInputStream(jarShellScriptPath);
                FileOutputStream out = new FileOutputStream(JOYConstants.START_JSTORM_SHELL);
                out.write(IOUtils.toByteArray(stream));
                out.close();
                jstormClientContext.shellScriptPath = JOYConstants.START_JSTORM_SHELL;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "No shell script specified to be executed by application master to start nimbus and supervisor");
            }
        } else if (cliParser.hasOption(JOYConstants.SHELL_COMMAND) && cliParser.hasOption(JOYConstants.SHELL_SCRIPT)) {
            throw new IllegalArgumentException("Can not specify shell_command option " +
                    "and shell_script option at the same time");
        } else if (cliParser.hasOption(JOYConstants.SHELL_COMMAND)) {
            jstormClientContext.shellCommand = cliParser.getOptionValue(JOYConstants.SHELL_COMMAND);
        } else {
            jstormClientContext.shellScriptPath = cliParser.getOptionValue(JOYConstants.SHELL_SCRIPT);
        }
        if (cliParser.hasOption(JOYConstants.SHELL_ARGS)) {
            jstormClientContext.shellArgs = cliParser.getOptionValues(JOYConstants.SHELL_ARGS);
        }
        setShellEnv(cliParser, jstormClientContext);
        jstormClientContext.shellCmdPriority = Integer.parseInt(cliParser.getOptionValue(JOYConstants.SHELL_CMD_PRIORITY, JOYConstants.SHELL_CMD_PRIORITY_DEFAULT_VALUE));
        //set AM memory default to 1000mb
        jstormClientContext.containerMemory = Integer.parseInt(cliParser.getOptionValue(JOYConstants.CONTAINER_MEMORY, JOYConstants.DEFAULT_CONTAINER_MEMORY));
        jstormClientContext.containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(JOYConstants.CONTAINER_VCORES, JOYConstants.DEFAULT_CONTAINER_VCORES));
        jstormClientContext.numContainers = Integer.parseInt(cliParser.getOptionValue(JOYConstants.NUM_CONTAINERS, JOYConstants.DEFAULT_NUM_CONTAINER));

        if (jstormClientContext.containerMemory < 0 || jstormClientContext.containerVirtualCores < 0 || jstormClientContext.numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + jstormClientContext.containerMemory
                    + ", containerVirtualCores=" + jstormClientContext.containerVirtualCores
                    + ", numContainer=" + jstormClientContext.numContainers);
        }

        jstormClientContext.nodeLabelExpression = cliParser.getOptionValue(JOYConstants.NODE_LABEL_EXPRESSION, null);
        jstormClientContext.clientTimeout = Integer.parseInt(cliParser.getOptionValue(JOYConstants.TIMEOUT, JOYConstants.DEFAULT_CLIENT_TIME_OUT));

        jstormClientContext.attemptFailuresValidityInterval =
                Long.parseLong(cliParser.getOptionValue(
                        JOYConstants.ATTEMPT_FAILURES_VALIDITY_INTERVAL, JOYConstants.DEFAULT_ATTEMPT_FAILURES_VALIDITY_INTERVAL));

        jstormClientContext.log4jPropFile = cliParser.getOptionValue(JOYConstants.LOG_PROPERTIES, JOYConstants.EMPTY);

        // Get timeline domain options
        if (cliParser.hasOption(JOYConstants.DOMAIN)) {
            jstormClientContext.domainId = cliParser.getOptionValue(JOYConstants.DOMAIN);
            jstormClientContext.toCreateDomain = cliParser.hasOption(JOYConstants.CREATE);
            if (cliParser.hasOption(JOYConstants.VIEW_ACLS)) {
                jstormClientContext.viewACLs = cliParser.getOptionValue(JOYConstants.VIEW_ACLS);
            }
            if (cliParser.hasOption(JOYConstants.MODIFY_ACLS)) {
                jstormClientContext.modifyACLs = cliParser.getOptionValue(JOYConstants.MODIFY_ACLS);
            }
        }
    }

    public static void setShellEnv(CommandLine cliParser, MasterContext jstormContext) {
        if (cliParser.hasOption(JOYConstants.SHELL_ENV)) {
            String envs[] = cliParser.getOptionValues(JOYConstants.SHELL_ENV);
            for (String env : envs) {
                env = env.trim();
                int index = env.indexOf(JOYConstants.EQUAL);
                if (index == -1) {
                    jstormContext.getShellEnv().put(env, JOYConstants.EMPTY);
                    continue;
                }
                String key = env.substring(0, index);
                String val = JOYConstants.EMPTY;
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                jstormContext.getShellEnv().put(key, val);
            }
        }
    }

    private static boolean fileExist(String filePath) {
        return new File(filePath).exists();
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private static void dumpOutDebugInfo() {

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
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            org.apache.hadoop.io.IOUtils.cleanup(LOG, buf);
        }
    }

    private static String readContent(String filePath) throws IOException {
        DataInputStream ds = null;
        try {
            ds = new DataInputStream(new FileInputStream(filePath));
            return ds.readUTF();
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(ds);
        }
    }

    public static void checkAndSetMasterOptions(CommandLine cliParser, JstormMasterContext jstormMasterContext, Configuration conf) throws Exception {

        //Check whether customer log4j.properties file exists
        if (fileExist(log4jPath)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(JstormMaster.class,
                        log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption(JOYConstants.DEBUG)) {
            dumpOutDebugInfo();
        }

        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption(JOYConstants.APP_ATTEMPT_ID)) {
                String appIdStr = cliParser.getOptionValue(JOYConstants.APP_ATTEMPT_ID, JOYConstants.EMPTY);
                jstormMasterContext.appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
            jstormMasterContext.appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + jstormMasterContext.appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + jstormMasterContext.appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + jstormMasterContext.appAttemptID.getAttemptId());

        if (!fileExist(shellCommandPath)
                && envs.get(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION).isEmpty()) {
            throw new IllegalArgumentException(
                    "No shell command or shell script specified to be executed by application master");
        }

        if (fileExist(shellCommandPath)) {
            jstormMasterContext.shellCommand = readContent(shellCommandPath);
        }

        if (fileExist(shellArgsPath)) {
            jstormMasterContext.shellArgs = readContent(shellArgsPath);
        }

        JstormYarnUtils.setShellEnv(cliParser, jstormMasterContext);

        if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
            jstormMasterContext.scriptPath = envs.get(JOYConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

            jstormMasterContext.appMasterJarPath = envs.get(JOYConstants.APPMASTERJARSCRIPTLOCATION);
            if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
                jstormMasterContext.shellScriptPathTimestamp = Long.parseLong(envs
                        .get(JOYConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
                jstormMasterContext.jarTimestamp = Long.parseLong(envs
                        .get(JOYConstants.APPMASTERTIMESTAMP));
            }
            if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
                jstormMasterContext.shellScriptPathLen = Long.parseLong(envs
                        .get(JOYConstants.DISTRIBUTEDSHELLSCRIPTLEN));
                jstormMasterContext.jarPathLen = Long.parseLong(envs
                        .get(JOYConstants.APPMASTERLEN));
            }

            if (!jstormMasterContext.scriptPath.isEmpty()
                    && (jstormMasterContext.shellScriptPathTimestamp <= 0 || jstormMasterContext.shellScriptPathLen <= 0)) {
                LOG.error("Illegal values in env for shell script path" + ", path="
                        + jstormMasterContext.scriptPath + ", len=" + jstormMasterContext.shellScriptPathLen + ", timestamp="
                        + jstormMasterContext.shellScriptPathTimestamp);
                throw new IllegalArgumentException(
                        "Illegal values in env for shell script path");
            }
        }

        if (envs.containsKey(JOYConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN)) {
            jstormMasterContext.domainId = envs.get(JOYConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN);
        }

        if (envs.containsKey(JOYConstants.BINARYFILEDEPLOYPATH)
                && !envs.get(JOYConstants.BINARYFILEDEPLOYPATH).equals(JOYConstants.EMPTY)) {
            conf.set(JOYConstants.INSTANCE_DEPLOY_DIR_KEY, envs.get(JOYConstants.BINARYFILEDEPLOYPATH));
            jstormMasterContext.deployPath = envs.get(JOYConstants.BINARYFILEDEPLOYPATH);
        }

        if (envs.containsKey(JOYConstants.INSTANCENAME)
                && !envs.get(JOYConstants.INSTANCENAME).equals(JOYConstants.EMPTY)) {
            conf.set(JOYConstants.INSTANCE_NAME_KEY, envs.get(JOYConstants.INSTANCENAME));
            jstormMasterContext.instanceName = envs.get(JOYConstants.INSTANCENAME);
        }
        jstormMasterContext.containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
                JOYConstants.CONTAINER_VCORES, JOYConstants.DEFAULT_CONTAINER_VCORES));
        jstormMasterContext.numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
                JOYConstants.NUM_CONTAINERS, JOYConstants.DEFAULT_NUM_CONTAINER));
        if (jstormMasterContext.numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run distributed shell with no containers");
        }
    }
}
