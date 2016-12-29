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
package com.alibaba.jstorm.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.management.ObjectName;

import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricType;

import backtype.storm.Config;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.NimbusClientWrapper;
import backtype.storm.utils.Utils;

/**
 * JStorm utility
 *
 * @author yannian/Longda/Xin.Zhou/Xin.Li
 */
@SuppressWarnings("unused")
public class JStormUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JStormUtils.class);

    public static long SIZE_1_K = 1024;
    public static long SIZE_1_M = SIZE_1_K * 1024;
    public static long SIZE_1_G = SIZE_1_M * 1024;
    public static long SIZE_1_T = SIZE_1_G * 1024;
    public static long SIZE_1_P = SIZE_1_T * 1024;

    public static final int MIN_1 = 60;
    public static final int MIN_10 = MIN_1 * 10;
    public static final int HOUR_1 = MIN_10 * 6;
    public static final int DAY_1 = HOUR_1 * 24;

    public static final String DEFAULT_BLOB_VERSION_SUFFIX = ".version";
    public static final String CURRENT_BLOB_SUFFIX_ID = "current";
    public static final String DEFAULT_CURRENT_BLOB_SUFFIX = "." + CURRENT_BLOB_SUFFIX_ID;
    private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<TSerializer>();
    private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<TDeserializer>();

    public static String getErrorInfo(String baseInfo, Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return baseInfo + "\r\n" + sw.toString() + "\r\n";
        } catch (Exception e2) {
            return baseInfo;
        }
    }

    public static String getErrorInfo(Throwable error) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            error.printStackTrace(pw);
            return sw.toString();
        } catch (Exception e1) {
            return "";
        }
    }

    /**
     * filter the map
     */
    public static <K, V> Map<K, V> select_keys_pred(Set<K> filter, Map<K, V> all) {
        Map<K, V> filterMap = new HashMap<K, V>();

        for (Entry<K, V> entry : all.entrySet()) {
            if (!filter.contains(entry.getKey())) {
                filterMap.put(entry.getKey(), entry.getValue());
            }
        }

        return filterMap;
    }

    public static byte[] barr(byte v) {
        byte[] byteArray = new byte[1];
        byteArray[0] = v;

        return byteArray;
    }

    public static byte[] barr(Short v) {
        byte[] byteArray = new byte[Short.SIZE / 8];
        for (int i = 0; i < byteArray.length; i++) {
            int off = (byteArray.length - 1 - i) * 8;
            byteArray[i] = (byte) ((v >> off) & 0xFF);
        }
        return byteArray;
    }

    public static byte[] barr(Integer v) {
        byte[] byteArray = new byte[Integer.SIZE / 8];
        for (int i = 0; i < byteArray.length; i++) {
            int off = (byteArray.length - 1 - i) * 8;
            byteArray[i] = (byte) ((v >> off) & 0xFF);
        }
        return byteArray;
    }

    // for test
    public static int byteToInt2(byte[] b) {

        int iOutcome = 0;
        byte bLoop;

        for (int i = 0; i < 4; i++) {
            bLoop = b[i];
            int off = (b.length - 1 - i) * 8;
            iOutcome += (bLoop & 0xFF) << off;

        }

        return iOutcome;
    }

    /**
     * this variable isn't clean, it makes JStormUtils ugly
     */
    public static boolean localMode = false;

    public static void setLocalMode(boolean localMode) {
        JStormUtils.localMode = localMode;
    }

    public static void haltProcess(int val) {
        Runtime.getRuntime().halt(val);
    }

    public static void halt_process(int val, String msg) {
        LOG.info("Halting process: " + msg);
        JStormUtils.sleepMs(1000);
        if (localMode && val == 0) {
            // throw new RuntimeException(msg);
        } else {
            haltProcess(val);
        }
    }

    /**
     * "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
     */
    public static <K, V> HashMap<V, List<K>> reverse_map(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);

        }

        return rtn;
    }

    /**
     * Gets the pid of current JVM, because Java doesn't provide a real way to do this.
     */
    public static String process_pid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        if (split.length != 2) {
            throw new RuntimeException("Got unexpected process name: " + name);
        }

        return split[0];
    }

    /**
     * use launchProcess to execute a command
     *
     * @param command command to be executed
     * @throws ExecuteException
     * @throws IOException
     */
    public static void exec_command(String command) throws ExecuteException, IOException {
        launchProcess(command, new HashMap<String, String>(), false);
    }


    /**
     * Extract dir from the jar to dest dir
     *
     * @param jarpath path to jar
     * @param dir     dir to be extracted
     * @param destdir destination dir
     */
    public static void extractDirFromJar(String jarpath, String dir, String destdir) {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(jarpath);
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries != null && entries.hasMoreElements()) {
                ZipEntry ze = entries.nextElement();
                if (!ze.isDirectory() && ze.getName().startsWith(dir)) {
                    InputStream in = zipFile.getInputStream(ze);
                    try {
                        File file = new File(destdir, ze.getName());
                        if (!file.getParentFile().mkdirs()) {
                            if (!file.getParentFile().isDirectory()) {
                                throw new IOException("Mkdirs failed to create " +
                                        file.getParentFile().toString());
                            }
                        }
                        OutputStream out = new FileOutputStream(file);
                        try {
                            byte[] buffer = new byte[8192];
                            int i;
                            while ((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        if (in != null)
                            in.close();
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("No " + dir + " from " + jarpath + "!\n" + e.getMessage());
        } finally {
            if (zipFile != null)
                try {
                    zipFile.close();
                } catch (Exception e) {
                    LOG.warn(e.getMessage());
                }

        }
    }

    public static void ensure_process_killed(Integer pid) {
        // just kill the process 5 times
        // to make sure the process is killed ultimately
        for (int i = 0; i < 5; i++) {
            try {
                exec_command("kill -9 " + pid);
                LOG.info("kill -9 process " + pid);
                sleepMs(100);
            } catch (ExecuteException e) {
                LOG.info("Error when trying to kill " + pid + ". Process has been killed");
            } catch (Exception e) {
                LOG.info("Error when trying to kill " + pid + ".Exception ", e);
            }
        }
    }

    public static void process_killed(Integer pid) {
        try {
            exec_command("kill " + pid);
            LOG.info("kill process " + pid);
        } catch (ExecuteException e) {
            LOG.info("Error when trying to kill " + pid + ". Process has been killed. ");
        } catch (Exception e) {
            LOG.info("Error when trying to kill " + pid + ".Exception ", e);
        }
    }

    public static void kill(Integer pid) {
        process_killed(pid);

        sleepMs(2 * 1000);

        ensure_process_killed(pid);
    }

    public static void kill_signal(Integer pid, String signal) {
        String cmd = "kill " + signal + " " + pid;
        try {
            exec_command(cmd);
            LOG.info(cmd);
        } catch (ExecuteException e) {
            LOG.info("Error when run " + cmd + ". Process has been killed. ");
        } catch (Exception e) {
            LOG.info("Error when run " + cmd + ". Exception ", e);
        }
    }

    /**
     * This function is only for linux
     */
    public static boolean isProcDead(String pid) {
        if (!OSInfo.isLinux()) {
            return false;
        }

        String path = "/proc/" + pid;
        File file = new File(path);

        if (!file.exists()) {
            LOG.info("Process " + pid + " is dead");
            return true;
        }

        return false;
    }

    public static Double getCpuUsage() {
        if (!OSInfo.isLinux()) {
            return 0.0;
        }

        Double value;
        String output = null;
        try {
            String pid = JStormUtils.process_pid();
            String command = String.format("top -b -n 1 -p %s | grep -w %s", pid, pid);
            output = SystemOperation.exec(command);
            String subStr = output.substring(output.indexOf("S") + 1);
            for (int i = 0; i < subStr.length(); i++) {
                char ch = subStr.charAt(i);
                if (ch != ' ') {
                    subStr = subStr.substring(i);
                    break;
                }
            }
            String usedCpu = subStr.substring(0, subStr.indexOf(" "));
            value = Double.valueOf(usedCpu);
        } catch (Exception e) {
            LOG.warn("Failed to get cpu usage ratio.");
            if (output != null)
                LOG.warn("Output string is \"" + output + "\"");
            value = 0.0;
        }

        return value;
    }

    public static Double getFullGC() {
        Double value = 0.0;
        long timeOut = 3000;
        if (!OSInfo.isLinux()) {
            return value;
        }

        String output = null;
        try {
            String targetPid = JStormUtils.process_pid();
            String command = String.format("jstat -gc %s", targetPid);

            List<String> commands = new ArrayList<String>();
            commands.add("/bin/bash");
            commands.add("-c");
            commands.add(command);


            Process process = null;

            try {
                process = launchProcess(commands, new HashMap<String, String>());

                StringBuilder sb = new StringBuilder();
                output = JStormUtils.getOutput(process.getInputStream());
                String errorOutput = JStormUtils.getOutput(process.getErrorStream());
                sb.append(output);
                sb.append("\n");
                sb.append(errorOutput);
                long start = System.currentTimeMillis();

                while (isAlive(process)){
                    Utils.sleep(100);
                    if (System.currentTimeMillis() - start > timeOut){
                        process.destroy();
                    }
                }
                Utils.sleep(100);
                int ret = process.exitValue();
                if (ret != 0) {
                    LOG.warn(command + " is terminated abnormally. ret={}, str={}", ret, sb.toString());
                }

            } catch (Throwable e) {
                LOG.error("Failed to run " + command + ", " + e.getCause(), e);
            }


            if (output != null) {
                String[] lines = output.split("[\\r\\n]+");
                if (lines.length >= 2) {
                    String[] headStrArray = lines[0].split("\\s+");
                    String[] valueStrArray = lines[1].split("\\s+");
                    List<String> filterHeads = new ArrayList<>();
                    List<String> filterValues = new ArrayList<>();

                    for (String string : headStrArray){
                        if (string.trim().length() == 0) {
                            continue;
                        }
                        filterHeads.add(string);
                    }
                    for (String string : valueStrArray){
                        if (string.trim().length() == 0) {
                            continue;
                        }
                        filterValues.add(string);
                    }
                    for (int i = 0; i < filterHeads.size(); i++){
                        String info = filterHeads.get(i);
                        LOG.debug("jstat gc {} {}", filterHeads.get(i), filterValues.get(i));
                        if (info.trim().length() != 0 && info.equals("FGC")) {
                            value = Double.parseDouble(filterValues.get(i));
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to get full gc.");
            if (output != null)
                LOG.warn("Output string is \"" + output + "\"");
        }
        return value;
    }

    public static double getTotalCpuUsage() {
        if (!OSInfo.isLinux()) {
            return 0.0;
        }
        return LinuxResource.getTotalCpuUsage();
    }

    public static Double getDiskUsage() {
        return LinuxResource.getDiskUsage();
    }

    public static double getTotalMemUsage() {
        if (!OSInfo.isLinux()) {
            return 0.0;
        }
        return LinuxResource.getTotalMemUsage();
    }

    public static Long getFreePhysicalMem() {
        return LinuxResource.getFreePhysicalMem();
    }

    public static int getNumProcessors() {
        return LinuxResource.getProcessNum();
    }

    public static Double getMemUsage() {
        if (OSInfo.isLinux()) {
            try {
                Double value;
                String pid = JStormUtils.process_pid();
                String command = String.format("top -b -n 1 -p %s | grep -w %s", pid, pid);
                String output = SystemOperation.exec(command);

                int m = 0;
                String[] strArray = output.split(" ");
                for (int i = 0; i < strArray.length; i++) {
                    String info = strArray[i];
                    if (info.trim().length() == 0) {
                        continue;
                    }
                    if (m == 5) {
                        // memory
                        String unit = info.substring(info.length() - 1);

                        if (unit.equalsIgnoreCase("g")) {
                            value = Double.parseDouble(info.substring(0, info.length() - 1));
                            value *= 1000000000;
                        } else if (unit.equalsIgnoreCase("m")) {
                            value = Double.parseDouble(info.substring(0, info.length() - 1));
                            value *= 1000000;
                        } else {
                            value = Double.parseDouble(info);
                        }

                        //LOG.info("!!!! Get Memory Size:{}, info:{}", value, info);
                        return value;
                    }
                    if (m == 8) {
                        // cpu usage
                    }
                    if (m == 9) {
                        // memory ratio
                    }
                    m++;
                }
            } catch (Exception e) {
                LOG.warn("Failed to get memory usage .");
            }
        }

        // this will be incorrect
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

        return (double) memoryUsage.getUsed();
    }

    public static double getJVMHeapMemory() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

        return (double) memoryUsage.getUsed();
    }

    /**
     * NOTE: DO NOT use Hook signal in JVM, it may cause JVM to exit
     */
    public static void registerJStormSignalHandler() {
        if (!OSInfo.isLinux()) {
            LOG.info("Skip register signal for current OS");
            return;
        }

        JStormSignalHandler instance = JStormSignalHandler.getInstance();
        int[] signals = {
                1, //SIGHUP
                2, //SIGINT
                //3, //Signal already used by VM or OS: SIGQUIT
                //4, //Signal already used by VM or OS: SIGILL
                5, //SIGTRAP
                6, //SIGABRT
                7, // SIGBUS
                //8, //Signal already used by VM or OS: SIGFPE
                //10, //Signal already used by VM or OS: SIGUSR1
                //11, Signal already used by VM or OS: SIGSEGV
                12, //SIGUSER2
                14, //SIGALM
                16, //SIGSTKFLT
        };


        for (int signal : signals) {
            instance.registerSignal(signal, null, true);
        }

    }

    /**
     * If it is backend, please set resultHandler, such as DefaultExecuteResultHandler
     * If it is frontend, ByteArrayOutputStream.toString will return the calling result
     * <p/>
     * This function will ignore whether the command is successfully executed or not
     *
     * @param command       command to be executed
     * @param environment   env vars
     * @param workDir       working directory
     * @param resultHandler exec result handler
     * @return output stream
     * @throws IOException
     */
    @Deprecated
    public static ByteArrayOutputStream launchProcess(
            String command, final Map environment, final String workDir,
            ExecuteResultHandler resultHandler) throws IOException {

        String[] cmdlist = command.split(" ");

        CommandLine cmd = new CommandLine(cmdlist[0]);
        for (String cmdItem : cmdlist) {
            if (!StringUtils.isBlank(cmdItem)) {
                cmd.addArgument(cmdItem);
            }
        }

        DefaultExecutor executor = new DefaultExecutor();

        executor.setExitValue(0);
        if (!StringUtils.isBlank(workDir)) {
            executor.setWorkingDirectory(new File(workDir));
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        PumpStreamHandler streamHandler = new PumpStreamHandler(out, out);
        executor.setStreamHandler(streamHandler);

        try {
            if (resultHandler == null) {
                executor.execute(cmd, environment);
            } else {
                executor.execute(cmd, environment, resultHandler);
            }
        } catch (ExecuteException ignored) {
        }

        return out;

    }

    protected static java.lang.Process launchProcess(final List<String> cmdlist,
                                                     final Map<String, String> environment) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(cmdlist);
        builder.redirectErrorStream(true);
        Map<String, String> process_evn = builder.environment();
        for (Entry<String, String> entry : environment.entrySet()) {
            process_evn.put(entry.getKey(), entry.getValue());
        }

        return builder.start();
    }

    public static String getOutput(InputStream input) {
        BufferedReader in = new BufferedReader(new InputStreamReader(input));
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            while ((line = in.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }


    public static String launchProcess(final String command, final List<String> cmdlist,
                                       final Map<String, String> environment, boolean backend) throws IOException {
        if (backend) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    List<String> cmdWrapper = new ArrayList<String>();

                    cmdWrapper.add("nohup");
                    cmdWrapper.addAll(cmdlist);
                    cmdWrapper.add("&");

                    try {
                        launchProcess(cmdWrapper, environment);
                    } catch (IOException e) {
                        LOG.error("Failed to run nohup " + command + " &," + e.getCause(), e);
                    }
                }
            }).start();
            return null;
        } else {
            try {
                Process process = launchProcess(cmdlist, environment);


                StringBuilder sb = new StringBuilder();
                String output = JStormUtils.getOutput(process.getInputStream());
                String errorOutput = JStormUtils.getOutput(process.getErrorStream());
                sb.append(output);
                sb.append("\n");
                sb.append(errorOutput);

                int ret = process.waitFor();
                if (ret != 0) {
                    LOG.warn(command + " is terminated abnormally. ret={}, str={}", ret, sb.toString());
                }
                return sb.toString();
            } catch (Throwable e) {
                LOG.error("Failed to run " + command + ", " + e.getCause(), e);
            }

            return "";
        }
    }

    /**
     * it should use DefaultExecutor to start a process,
     * but some little problem have been found,
     * such as exitCode/output string so still use the old method to start process
     *
     * @param command     command to be executed
     * @param environment env vars
     * @param backend     whether the command is executed at backend
     * @return outputString
     * @throws IOException
     */
    public static String launchProcess(final String command,
                                       final Map<String, String> environment, boolean backend) throws IOException {
        String[] cmds = command.split(" ");

        ArrayList<String> cmdList = new ArrayList<>();
        for (String tok : cmds) {
            if (!StringUtils.isBlank(tok)) {
                cmdList.add(tok);
            }
        }

        return launchProcess(command, cmdList, environment, backend);
    }

    public static String current_classpath() {
        return System.getProperty("java.class.path");
    }

    public static String to_json(Map m) {
        return Utils.to_json(m);
    }

    public static Object from_json(String json) {
        return Utils.from_json(json);
    }

    public static <V> HashMap<V, Integer> multi_set(List<V> list) {
        HashMap<V, Integer> rtn = new HashMap<V, Integer>();
        for (V v : list) {
            int cnt = 1;
            if (rtn.containsKey(v)) {
                cnt += rtn.get(v);
            }
            rtn.put(v, cnt);
        }
        return rtn;
    }

    /**
     * if the list contains repeated string, return the repeated string
     * this function is used to check whether bolt/spout has a duplicate id
     */
    public static List<String> getRepeat(List<String> list) {
        List<String> rtn = new ArrayList<String>();
        Set<String> idSet = new HashSet<String>();

        for (String id : list) {
            if (idSet.contains(id)) {
                rtn.add(id);
            } else {
                idSet.add(id);
            }
        }

        return rtn;
    }

    /**
     * balance all T
     */
    public static <T> List<T> interleave_all(List<List<T>> splitup) {
        ArrayList<T> rtn = new ArrayList<T>();
        int maxLength = 0;
        for (List<T> e : splitup) {
            int len = e.size();
            if (maxLength < len) {
                maxLength = len;
            }
        }

        for (int i = 0; i < maxLength; i++) {
            for (List<T> e : splitup) {
                if (e.size() > i) {
                    rtn.add(e.get(i));
                }
            }
        }

        return rtn;
    }

    public static long bit_xor_vals(Object... vals) {
        long rtn = 0l;
        for (Object n : vals) {
            rtn = bit_xor(rtn, n);
        }

        return rtn;
    }

    public static <T> long bit_xor_vals(java.util.List<T> vals) {
        long rtn = 0l;
        for (T n : vals) {
            rtn = bit_xor(rtn, n);
        }

        return rtn;
    }

    public static <T> long bit_xor_vals_sets(java.util.Set<T> vals) {
        long rtn = 0l;
        for (T n : vals) {
            rtn = bit_xor(rtn, n);
        }
        return rtn;
    }

    public static long bit_xor(Object a, Object b) {
        long rtn;

        if (a instanceof Long && b instanceof Long) {
            rtn = ((Long) a) ^ ((Long) b);
            return rtn;
        } else if (b instanceof Set) {
            long bs = bit_xor_vals_sets((Set) b);
            return bit_xor(a, bs);
        } else if (a instanceof Set) {
            long as = bit_xor_vals_sets((Set) a);
            return bit_xor(as, b);
        } else {
            long ai = Long.parseLong(String.valueOf(a));
            long bi = Long.parseLong(String.valueOf(b));
            rtn = ai ^ bi;
            return rtn;
        }

    }

    public static <V> List<V> mk_list(V... args) {
        ArrayList<V> rtn = new ArrayList<V>();
        for (V o : args) {
            rtn.add(o);
        }
        return rtn;
    }

    public static <V> List<V> mk_list(java.util.Set<V> args) {
        ArrayList<V> rtn = new ArrayList<V>();
        if (args != null) {
            for (V o : args) {
                rtn.add(o);
            }
        }
        return rtn;
    }

    public static <V> List<V> mk_list(Collection<V> args) {
        ArrayList<V> rtn = new ArrayList<V>();
        if (args != null) {
            for (V o : args) {
                rtn.add(o);
            }
        }
        return rtn;
    }

    public static <V> V[] mk_arr(V... args) {
        return args;
    }

    public static Long parseLong(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Long.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Integer value = (Integer) o;
            return Long.valueOf(value);
        } else if (o instanceof Long) {
            return (Long) o;
        } else {
            throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
        }
    }

    public static Double parseDouble(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Double.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Number value = (Integer) o;
            return value.doubleValue();
        } else if (o instanceof Long) {
            Number value = (Long) o;
            return value.doubleValue();
        } else if (o instanceof Double) {
            return (Double) o;
        } else {
            throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
        }
    }

    public static Double parseDouble(Object o, double defaultValue) {
        if (o == null) {
            return defaultValue;
        }
        try {
            return parseDouble(o);
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    public static Long parseLong(Object o, long defaultValue) {

        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return Long.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Integer value = (Integer) o;
            return Long.valueOf(value);
        } else if (o instanceof Long) {
            return (Long) o;
        } else {
            return defaultValue;
        }
    }

    public static Integer parseInt(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof String) {
            return Integer.parseInt(String.valueOf(o));
        } else {
            throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
        }
    }

    public static Integer parseInt(Object o, int defaultValue) {
        if (o == null) {
            return defaultValue;
        }
        if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof String) {
            return Integer.parseInt(String.valueOf(o));
        } else {
            return defaultValue;
        }
    }

    public static Boolean parseBoolean(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Boolean.valueOf((String) o);
        } else if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            throw new RuntimeException("Invalid value " + o.getClass().getName() + " " + o);
        }
    }

    public static boolean parseBoolean(Object o, boolean defaultValue) {
        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return Boolean.valueOf((String) o);
        } else if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            return defaultValue;
        }
    }

    public static <V> Set<V> listToSet(List<V> list) {
        if (list == null) {
            return null;
        }

        Set<V> set = new HashSet<V>();
        set.addAll(list);
        return set;
    }

    /**
     * Check whether the zipfile contains the resources
     */
    public static boolean zipContainsDir(String zipfile, String resources) {
        Enumeration<? extends ZipEntry> entries;
        try {
            entries = (new ZipFile(zipfile)).entries();
            while (entries != null && entries.hasMoreElements()) {
                ZipEntry ze = entries.nextElement();
                String name = ze.getName();
                if (name.startsWith(resources + "/")) {
                    return true;
                }
            }
        } catch (IOException e) {
            LOG.error("zipContainsDir error", e);
        }

        return false;
    }

    public static Object add(Object oldValue, Object newValue) {
        if (oldValue == null) {
            return newValue;
        } else if (newValue == null) {
            return oldValue;
        }

        if (oldValue instanceof Long) {
            return (Long) oldValue + (Long) newValue;
        } else if (oldValue instanceof Double) {
            return (Double) oldValue + (Double) newValue;
        } else if (oldValue instanceof Integer) {
            return (Integer) oldValue + (Integer) newValue;
        } else if (oldValue instanceof Float) {
            return (Float) oldValue + (Float) newValue;
        } else if (oldValue instanceof Short) {
            return (Short) oldValue + (Short) newValue;
        } else if (oldValue instanceof BigInteger) {
            return ((BigInteger) oldValue).add((BigInteger) newValue);
        } else if (oldValue instanceof Number) {
            return ((Number) oldValue).doubleValue() + ((Number) newValue).doubleValue();
        } else {
            return null;
        }
    }

    public static Object mergeList(List<Object> list) {
        Object ret = null;

        for (Object value : list) {
            ret = add(ret, value);
        }

        return ret;
    }

    public static List<Object> mergeList(List<Object> result, Object add) {
        if (add instanceof Collection) {
            for (Object o : (Collection) add) {
                result.add(o);
            }
        } else if (add instanceof Set) {
            for (Object o : (Collection) add) {
                result.add(o);
            }
        } else {
            result.add(add);
        }

        return result;
    }

    public static List<Object> distinctList(List<Object> input) {
        List<Object> retList = new ArrayList<Object>();

        for (Object object : input) {
            if (!retList.contains(object)) {
                retList.add(object);
            }
        }

        return retList;
    }

    public static <K, V> Map<K, V> mergeMapList(List<Map<K, V>> list) {
        Map<K, V> ret = new HashMap<K, V>();

        for (Map<K, V> listEntry : list) {
            if (listEntry == null) {
                continue;
            }
            for (Entry<K, V> mapEntry : listEntry.entrySet()) {
                K key = mapEntry.getKey();
                V value = mapEntry.getValue();

                V retValue = (V) add(ret.get(key), value);

                ret.put(key, retValue);
            }
        }

        return ret;
    }

    public static String formatSimpleDouble(Double value) {
        try {
            java.text.DecimalFormat form = new java.text.DecimalFormat("##0.000");
            String s = form.format(value);
            return s;
        } catch (Exception e) {
            return "0.000";
        }

    }

    public static double formatDoubleDecPoint2(Double value) {
        try {
            java.text.DecimalFormat form = new java.text.DecimalFormat("##.00");
            String s = form.format(value);
            return Double.valueOf(s);
        } catch (Exception e) {
            return 0.0;
        }
    }

    public static double formatDoubleDecPoint4(Double value) {
        try {
            java.text.DecimalFormat form = new java.text.DecimalFormat("###.0000");
            String s = form.format(value);
            return Double.valueOf(s);
        } catch (Exception e) {
            return 0.0;
        }
    }

    public static Double convertToDouble(Object value) {
        Double ret;

        if (value == null) {
            ret = null;
        } else {
            if (value instanceof Integer) {
                ret = ((Integer) value).doubleValue();
            } else if (value instanceof Long) {
                ret = ((Long) value).doubleValue();
            } else if (value instanceof Float) {
                ret = ((Float) value).doubleValue();
            } else if (value instanceof Double) {
                ret = (Double) value;
            } else {
                ret = null;
            }
        }

        return ret;
    }

    public static String formatValue(Object value) {
        if (value == null) {
            return "0";
        }

        if (value instanceof Long) {
            return String.valueOf(value);
        } else if (value instanceof Double) {
            return formatSimpleDouble((Double) value);
        } else {
            return String.valueOf(value);
        }
    }

    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }

    public static String HEXES = "0123456789ABCDEF";

    public static String toPrintableString(byte[] buf) {
        if (buf == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (byte b : buf) {
            if (index % 10 == 0) {
                sb.append("\n");
            }
            index++;

            sb.append(HEXES.charAt((b & 0xF0) >> 4));
            sb.append(HEXES.charAt((b & 0x0F)));
            sb.append(" ");
        }

        return sb.toString();
    }

    public static Long getPhysicMemorySize() {
        Object object;
        try {
            object = ManagementFactory.getPlatformMBeanServer().getAttribute(
                    new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize");
        } catch (Exception e) {
            LOG.warn("Failed to get system physical memory size,", e);
            return null;
        }

        return (Long) object;
    }

    public static String genLogName(String topology, Integer port) {
        return topology + "-worker-" + port + ".log";
    }

    // public static String getLog4jFileName(org.apache.log4j.Logger
    // log4jLogger) throws Exception{
    // Enumeration<org.apache.log4j.Appender> enumAppender =
    // log4jLogger.getAllAppenders();
    // org.apache.log4j.FileAppender fileAppender = null;
    // while (enumAppender.hasMoreElements()) {
    // org.apache.log4j.Appender appender = enumAppender.nextElement();
    // if (appender instanceof org.apache.log4j.FileAppender) {
    // fileAppender = (org.apache.log4j.FileAppender) appender;
    // break;
    // }
    // }
    // if (fileAppender != null) {
    // return fileAppender.getFile();
    //
    // }
    //
    // return null;
    // }

    public static String getLogFileName() {
        try {
            Logger rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            if (rootLogger instanceof ch.qos.logback.classic.Logger) {
                ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) rootLogger;
                // Logger framework is Logback
                for (Iterator<ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent>> index =
                     logbackLogger.iteratorForAppenders(); index.hasNext(); ) {
                    ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent> appender = index.next();
                    if (appender instanceof ch.qos.logback.core.FileAppender) {
                        ch.qos.logback.core.FileAppender fileAppender = (ch.qos.logback.core.FileAppender) appender;
                        return fileAppender.getFile();
                    }
                }
            }
            // else if (rootLogger instanceof org.slf4j.impl.Log4jLoggerAdapter)
            // {
            // // slf4j-log4j
            // org.slf4j.impl.Log4jLoggerAdapter log4jAdapter =
            // (org.slf4j.impl.Log4jLoggerAdapter) rootLogger;
            // try {
            // Field field = log4jAdapter.getClass().getDeclaredField(
            // "logger");
            // field.setAccessible(true);
            // Object obj = field.get(log4jAdapter);
            // if (obj instanceof org.apache.log4j.spi.RootLogger) {
            // return getLog4jFileName((org.apache.log4j.spi.RootLogger) obj);
            // }
            // } catch (Exception e) {
            // e.printStackTrace();
            // }
            //
            // } else if (rootLogger instanceof org.apache.log4j.Logger) {
            // return getLog4jFileName((org.apache.log4j.Logger) rootLogger);
            // }

        } catch (Throwable e) {
            LOG.info("Failed to get root logger file name", e.getMessage());
            return null;
        }
        return null;
    }

    public static String getLogDir() {
        String file = JStormUtils.getLogFileName();
        if (file != null) {
            if (file.lastIndexOf(File.separator) < 0)
                return "";
            return file.substring(0, file.lastIndexOf(File.separator));
        }

        String stormHome = System.getProperty("jstorm.home");
        if (stormHome == null) {
            return "." + File.separator + "logs";
        } else {
            return stormHome + File.separator + "logs";
        }
    }

    public static void redirectOutput(String file) throws Exception {
        System.out.println("Redirecting output to " + file);

        FileOutputStream workerOut = new FileOutputStream(new File(file));

        PrintStream ps = new PrintStream(new BufferedOutputStream(workerOut), true);
        System.setOut(ps);
        System.setErr(ps);

        LOG.info("Successfully redirect System.out to " + file);

    }

    public static RunnableCallback getDefaultKillfn() {
        return new AsyncLoopDefaultKill();
    }

    public static TreeMap<Integer, Integer> integer_divided(int sum, int num_pieces) {
        return Utils.integerDivided(sum, num_pieces);
    }

    public static <K, V> HashMap<K, V> filter_val(RunnableCallback fn, Map<K, V> amap) {
        HashMap<K, V> rtn = new HashMap<K, V>();

        for (Entry<K, V> entry : amap.entrySet()) {
            V value = entry.getValue();
            Object result = fn.execute(value);

            if (result == (Boolean) true) {
                rtn.put(entry.getKey(), value);
            }
        }
        return rtn;
    }

    public static int getSupervisorPortNum(Map conf, int sysCpuNum, Long physicalMemSize, boolean reserved) {
        double cpuWeight = ConfigExtension.getSupervisorSlotsPortCpuWeight(conf);

        int cpuPortNum = (int) (sysCpuNum / cpuWeight);

        if (cpuPortNum < 1) {
            LOG.info("Invalid supervisor.slots.port.cpu.weight setting :" + cpuWeight + ", cpu cores:" + sysCpuNum);
            cpuPortNum = 1;
        }

        Double memWeight = ConfigExtension.getSupervisorSlotsPortMemWeight(conf);

        int memPortNum = Integer.MAX_VALUE;

        if (physicalMemSize == null) {
            LOG.info("Failed to get memory size");
        } else {
            LOG.debug("Get system memory size: " + physicalMemSize);

            long workerMemSize = ConfigExtension.getMemSizePerWorker(conf);

            memPortNum = (int) (physicalMemSize / (workerMemSize * memWeight));

            if (memPortNum < 1) {
                LOG.info("Invalid worker.memory.size setting:" + workerMemSize);
                memPortNum = reserved ? 1 : 4;
            } else if (memPortNum < 4) {
                LOG.info("System memory is too small for Jstorm");
                memPortNum = reserved ? 1 : 4;
            }
        }

        return Math.min(cpuPortNum, memPortNum);
    }

    public static List<Integer> getSupervisorPortList(Map conf) {
        List<Integer> portList = (List<Integer>) conf.get(Config.SUPERVISOR_SLOTS_PORTS);

        if (portList != null && portList.size() > 0) {
            return new ArrayList<>(portList);
        }

        int sysCpuNum = 4;
        try {
            sysCpuNum = Runtime.getRuntime().availableProcessors();
        } catch (Exception e) {
            LOG.info("Failed to get CPU core num, set to 4");
        }

        Long physicalMemSize = JStormUtils.getPhysicMemorySize();

        if (physicalMemSize != null && physicalMemSize > 8589934592L) {
            long reserveMemory = ConfigExtension.getStormMachineReserveMem(conf);
            if (physicalMemSize < reserveMemory) {
                throw new RuntimeException("ReserveMemory is too large , PhysicalMemSize is:" + physicalMemSize);
            }
            physicalMemSize -= reserveMemory;
        }

        int portNum = getSupervisorPortNum(conf, sysCpuNum, physicalMemSize, false);

        int portBase = ConfigExtension.getSupervisorSlotsPortsBase(conf);

        portList = new ArrayList<>();
        for (int i = 0; i < portNum; i++) {
            portList.add(portBase + i);
        }

        return portList;
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }

    public static Object thriftToObject(Object obj) {
        Object ret;
        if (obj instanceof org.apache.thrift.TBase) {
            ret = thriftToMap((org.apache.thrift.TBase) obj);
        } else if (obj instanceof List) {
            ret = new ArrayList<>();
            for (Object item : (List) obj) {
                ((List) ret).add(thriftToObject(item));
            }
        } else if (obj instanceof Map) {
            ret = new HashMap<String, Object>();
            Set<Map.Entry> entrySet = ((Map) obj).entrySet();
            for (Map.Entry entry : entrySet) {
                ((Map) ret).put(String.valueOf(entry.getKey()), thriftToObject(entry.getValue()));
            }
        } else {

            ret = String.valueOf(obj);
        }

        return ret;
    }

    public static Map<String, Object> thriftToMap(
            org.apache.thrift.TBase thriftObj) {
        Map<String, Object> ret = new HashMap<String, Object>();

        int i = 1;
        TFieldIdEnum field = thriftObj.fieldForId(i);
        while (field != null) {
            if (thriftObj.isSet(field)) {
                Object obj = thriftObj.getFieldValue(field);
                ret.put(field.getFieldName(), thriftToObject(obj));

            }
            field = thriftObj.fieldForId(++i);
        }

        return ret;
    }

    public static List<Map<String, Object>> thriftToMap(List thriftObjs) {
        List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();

        for (Object thriftObj : thriftObjs) {
            ret.add(thriftToMap((org.apache.thrift.TBase) thriftObj));
        }

        return ret;
    }

    private static TDeserializer getDes() {
        TDeserializer des = threadDes.get();
        if (des == null) {
            des = new TDeserializer();
            threadDes.set(des);
        }
        return des;
    }

    private static TSerializer getSer() {
        TSerializer ser = threadSer.get();
        if (ser == null) {
            ser = new TSerializer();
            threadSer.set(ser);
        }
        return ser;
    }

    public static byte[] thriftSerialize(TBase t) {
        try {
            TSerializer ser = getSer();
            return ser.serialize(t);
        } catch (TException e) {
            LOG.error("Failed to serialize to thrift: ", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class c, byte[] b) {
        try {
            return thriftDeserialize(c, b, 0, b.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class c, byte[] b, int offset, int length) {
        try {
            T ret = (T) c.newInstance();
            TDeserializer des = getDes();
            des.deserialize((TBase) ret, b, offset, length);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map parseJson(String json) {
        if (json == null)
            return new HashMap();
        else
            return (Map) JSONValue.parse(json);
    }

    public static String mergeIntoJson(Map into, Map newMap) {
        Map res = new HashMap(into);
        if (newMap != null)
            res.putAll(newMap);
        return JSONValue.toJSONString(res);
    }

    /**
     * Get Topology Metrics
     *
     * @param topologyName topology name
     * @param metricType,  please refer to MetaType, default to MetaType.TASK.getT()
     * @param window,      please refer to AsmWindow, default to AsmWindow.M1_WINDOW
     * @return
     */
    public static Map<String, Double> getMetrics(Map conf, String topologyName, MetaType metricType, Integer window) {
        NimbusClientWrapper nimbusClient = null;
        Iface client = null;
        Map<String, Double> summary = new HashMap<>();

        try {
            nimbusClient = new NimbusClientWrapper();
            nimbusClient.init(conf);

            client = nimbusClient.getClient();

            String topologyId = client.getTopologyId(topologyName);

            if (metricType == null) {
                metricType = MetaType.TASK;
            }

            List<MetricInfo> allTaskMetrics = client.getMetrics(topologyId, metricType.getT());
            if (allTaskMetrics == null) {
                throw new RuntimeException("Failed to get metrics");
            }

            if (window == null || !AsmWindow.TIME_WINDOWS.contains(window)) {
                window = AsmWindow.M1_WINDOW;
            }

            for (MetricInfo taskMetrics : allTaskMetrics) {

                Map<String, Map<Integer, MetricSnapshot>> metrics = taskMetrics.get_metrics();
                if (metrics == null) {
                    System.out.println("Failed to get task metrics");
                    continue;
                }
                for (Entry<String, Map<Integer, MetricSnapshot>> entry : metrics.entrySet()) {
                    String key = entry.getKey();

                    MetricSnapshot metric = entry.getValue().get(window);
                    if (metric == null) {
                        throw new RuntimeException("Failed to get one minute metrics of " + key);
                    }

                    if (metric.get_metricType() == MetricType.COUNTER.getT()) {
                        summary.put(key, (double) metric.get_longValue());
                    } else if (metric.get_metricType() == MetricType.GAUGE.getT()) {
                        summary.put(key, metric.get_doubleValue());
                    } else {
                        summary.put(key, metric.get_mean());
                    }
                }
            }

            return summary;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                nimbusClient.cleanup();
            }
        }
    }

    public static void reportError(TopologyContext topologyContext, String errorMessge) {
        StormClusterState zkCluster = topologyContext.getZkCluster();
        String topologyId = topologyContext.getTopologyId();
        int taskId = topologyContext.getThisTaskId();

        try {
            zkCluster.report_task_error(topologyId, taskId, errorMessge);
        } catch (Exception e) {
            LOG.warn("Failed to report Error");
        }
    }

    public static boolean isKilledStatus(TopologyContext topologyContext) {
        boolean ret = false;
        StormClusterState zkCluster = topologyContext.getZkCluster();
        String topologyId = topologyContext.getTopologyId();
        try {
            StormBase stormBase = zkCluster.storm_base(topologyId, null);
            boolean isKilledStatus = stormBase != null && stormBase.getStatus().getStatusType().equals(StatusType.killed);
            ret = (stormBase == null || isKilledStatus);
        } catch (Exception e) {
            LOG.warn("Failed to get stormBase", e);
        }
        return ret;
    }

    public static String trimEnd(String s) {
        if (s == null) {
            return null;
        }

        int len = s.length();
        StringBuilder sb = new StringBuilder(len);
        int i = len - 1;
        for (; i >= 0; i--) {
            char ch = s.charAt(i);
            if (ch != ' ' && ch != '\r' && ch != '\n' && ch != '\t') {
                break;
            }
        }
        return sb.append(s.substring(0, i + 1)).toString();
    }

    public static boolean isAlive(Process process) {
        try {
            process.exitValue();
            return false;
        } catch (IllegalThreadStateException e) {
            return true;
        }
    }
}
