package com.alibaba.jstorm.yarn.utils;

import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.constants.JstormXmlConfKeys;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

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
}
