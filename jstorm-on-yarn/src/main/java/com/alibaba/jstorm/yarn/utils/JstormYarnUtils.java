package com.alibaba.jstorm.yarn.utils;

import com.alibaba.jstorm.yarn.constants.JstormKeys;
import com.alibaba.jstorm.yarn.constants.JstormXmlConfKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
        return join(getSupervisorPorts(memory, vcores, instanceName, supervisorHost, registryOperations), ",", false);
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
                "JstormOnYarn", JstormKeys.APP_TYPE);
        String path = RegistryUtils.servicePath(
                "JstormOnYarn", JstormKeys.APP_TYPE, instanceName);
        String hostPath = RegistryUtils.componentPath(
                "JstormOnYarn", JstormKeys.APP_TYPE, instanceName, supervisorHost);

        try {

            List<String> instanceNames = registryOperations.list(appPath);
            for (String instance : instanceNames) {
                String servicePath = RegistryUtils.servicePath(
                        "JstormOnYarn", JstormKeys.APP_TYPE, instance);
//                List<String> hosts = registryOperations.list(servicePath);
                Map<String, ServiceRecord> hosts = RegistryUtils.listServiceRecords(registryOperations, servicePath);
                for(String host:hosts.keySet())
                {
                    ServiceRecord sr = hosts.get("host");
                    String[] portList = sr.get("portList").split(",");
                    for (String usedport : portList) {
                        if (Integer.parseInt(usedport) == port)
                            return true;
                    }
                }

            }

            if (registryOperations.exists(path)) {
                ServiceRecord sr = registryOperations.resolve(path);
                String[] portList = sr.get("portList").split(",");

                String portListUpdate = join(portList, ",", true) + String.valueOf(port);
                sr.set("portList", portListUpdate);
                registryOperations.bind(path, sr, BindFlags.OVERWRITE);
                return false;
            } else {
                registryOperations.mknode(path, true);
                ServiceRecord sr = new ServiceRecord();
                String portListUpdate = String.valueOf(port);
                sr.set("portList", portListUpdate);
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
        LOG.info("memory is :" + String.valueOf(memory));
        LOG.info("vcores is :" + String.valueOf(vcores));
        LOG.info("slotCount is :" + String.valueOf(slotCount));
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
}
