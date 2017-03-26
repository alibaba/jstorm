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

package backtype.storm.utils;

import com.google.common.collect.Lists;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import backtype.storm.Config;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.StormTopology;
import backtype.storm.serialization.DefaultSerializationDelegate;
import backtype.storm.serialization.SerializationDelegate;
import clojure.lang.IFn;
import clojure.lang.RT;

@SuppressWarnings("unused,unchecked")
public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";
    private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<>();
    private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<>();

    private static SerializationDelegate serializationDelegate;
    private static ClassLoader cl = ClassLoader.getSystemClassLoader();

    static {
        Map conf = readStormConfig();
        serializationDelegate = getSerializationDelegate(conf);
    }

    public static Object newInstance(String klass) {
        try {
            Class c = Class.forName(klass);
            return c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object newInstance(String klass, Object... params) {
        try {
            Class c = Class.forName(klass);
            Constructor[] constructors = c.getConstructors();
            Constructor con = null;
            for (Constructor cons : constructors) {
                if (cons.getParameterTypes().length == params.length) {
                    con = cons;
                    break;
                }
            }

            if (con == null) {
                throw new RuntimeException("Cound not found the corresponding constructor, params=" + JStormUtils.mk_list(params));
            } else {
                if (con.getParameterTypes().length == 0) {
                    return c.newInstance();
                } else {
                    return con.newInstance(params);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Go thrift gzip serializer
     */
    public static byte[] serialize(Object obj) {
        /**
         * @@@ JStorm disable the thrift.gz.serializer
         */
        // return serializationDelegate.serialize(obj);
        return javaSerialize(obj);
    }

    /**
     * Go thrift gzip serializer
     */
    public static <T> T deserialize(byte[] serialized, Class<T> clazz) {
        /**
         * @@@ JStorm disable the thrift.gz.serializer
         */
        // return serializationDelegate.deserialize(serialized, clazz);
        return (T) javaDeserialize(serialized);
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

    public static byte[] javaSerialize(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object trySerialize(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return serialize(obj);
        } catch (Exception e) {
            LOG.info("Failed to serialize. cause={}", e.getCause());
            return null;
        }
    }

    public static Object maybe_deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return javaDeserializeWithCL(data, null);
        } catch (Exception e) {
            LOG.info("Failed to deserialize. cause={}", e.getCause());
            return null;
        }
    }

    /**
     * Deserialized with ClassLoader
     */
    public static Object javaDeserializeWithCL(byte[] serialized, URLClassLoader loader) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            Object ret = null;
            if (loader != null) {
                ClassLoaderObjectInputStream cis = new ClassLoaderObjectInputStream(loader, bis);
                ret = cis.readObject();
                cis.close();
            } else {
                ObjectInputStream ois = new ObjectInputStream(bis);
                ret = ois.readObject();
                ois.close();
            }
            return ret;
        } catch (IOException | ClassNotFoundException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object javaDeserialize(byte[] serialized) {
        return javaDeserializeWithCL(serialized, WorkerClassLoader.getInstance());
    }

    public static <T> T javaDeserialize(byte[] serialized, Class<T> clazz) {
        return (T) javaDeserializeWithCL(serialized, WorkerClassLoader.getInstance());
    }

    public static String to_json(Object m) {
        // return JSON.toJSONString(m);
        return JSONValue.toJSONString(m);
    }

    public static Object from_json(String json) {
        if (json == null) {
            return null;
        } else {
            // return JSON.parse(json);
            return JSONValue.parse(json);
        }
    }

    public static String toPrettyJsonString(Object obj) {
        Gson gson2 = new GsonBuilder().setPrettyPrinting().create();
        return gson2.toJson(obj);
    }

    public static byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream out = new GZIPOutputStream(bos);
            out.write(data);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] gunzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream in = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) >= 0) {
                bos.write(buffer, 0, len);
            }
            in.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toCompressedJsonConf(Map<String, Object> stormConf) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            OutputStreamWriter out = new OutputStreamWriter(new GZIPOutputStream(bos));
            JSONValue.writeJSONString(stormConf, out);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> fromCompressedJsonConf(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            InputStreamReader in = new InputStreamReader(new GZIPInputStream(bis));
            Object ret = JSONValue.parseWithException(in);
            in.close();
            return (Map<String, Object>) ret;
        } catch (IOException | ParseException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        StringBuilder ret = new StringBuilder();
        while (it.hasNext()) {
            ret.append(it.next());
            if (it.hasNext()) {
                ret.append(sep);
            }
        }
        return ret.toString();
    }

    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(String name) {
        return LoadConf.findAndReadYaml(name, true, false);
    }

    private static Map DEFAULT_CONF = null;
    public static Map readDefaultConfig() {
        synchronized(Utils.class) {
            if (DEFAULT_CONF == null) {
                DEFAULT_CONF = LoadConf.findAndReadYaml("defaults.yaml", true, false);
            }
        }
        
        return DEFAULT_CONF;
    }

    public static Map readCommandLineOpts() {
        Map ret = new HashMap();
        String commandOptions = System.getProperty("storm.options");
        if (commandOptions != null) {
            String[] configs = commandOptions.split(",");
            for (String config : configs) {
                config = URLDecoder.decode(config);
                String[] options = config.split("=", 2);
                if (options.length == 2) {
                    Object val = JSONValue.parse(options[1]);
                    if (val == null) {
                        val = options[1];
                    }
                    ret.put(options[0], val);
                }
            }
        }

        String excludeJars = System.getProperty("exclude.jars");
        if (excludeJars != null) {
            ret.put("exclude.jars", excludeJars);
        }

        /*
         * Trident and old transaction implementation do not work on batch mode. So, for the relative topology builder
         */
        String batchOptions = System.getProperty(ConfigExtension.TASK_BATCH_TUPLE);
        if (!StringUtils.isBlank(batchOptions)) {
            boolean isBatched = JStormUtils.parseBoolean(batchOptions, true);
            ConfigExtension.setTaskBatchTuple(ret, isBatched);
            System.out.println(ConfigExtension.TASK_BATCH_TUPLE + " is " + batchOptions);
        }
        String ackerOptions = System.getProperty(Config.TOPOLOGY_ACKER_EXECUTORS);
        if (!StringUtils.isBlank(ackerOptions)) {
            Integer ackerNum = JStormUtils.parseInt(ackerOptions, 0);
            ret.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackerNum);
            System.out.println(Config.TOPOLOGY_ACKER_EXECUTORS + " is " + ackerNum);
        }
        return ret;
    }

    public static void replaceLocalDir(Map<Object, Object> conf) {
        String stormHome = System.getProperty("jstorm.home");
        boolean isEmpty = StringUtils.isBlank(stormHome);

        Map<Object, Object> replaceMap = new HashMap<>();

        for (Entry entry : conf.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                if (StringUtils.isBlank((String) value)) {
                    continue;
                }

                String str = (String) value;
                if (isEmpty) {
                    // replace %JSTORM_HOME% as current directory
                    str = str.replace("%JSTORM_HOME%", ".");
                } else {
                    str = str.replace("%JSTORM_HOME%", stormHome);
                }

                replaceMap.put(key, str);
            }
        }

        conf.putAll(replaceMap);
    }

    public static Map loadDefinedConf(String confFile) {
        File file = new File(confFile);
        if (!file.exists()) {
            return LoadConf.findAndReadYaml(confFile, true, false);
        }

        Yaml yaml = new Yaml();
        Map ret;
        try {
            ret = (Map) yaml.load(new FileReader(file));
        } catch (FileNotFoundException e) {
            ret = null;
        }
        if (ret == null)
            ret = new HashMap();

        return new HashMap(ret);
    }

    public static Map readStormConfig() {
        Map ret = readDefaultConfig();
        String confFile = System.getProperty("storm.conf.file");
        Map storm;
        if (StringUtils.isBlank(confFile)) {
            storm = LoadConf.findAndReadYaml("storm.yaml", false, false);
        } else {
            storm = loadDefinedConf(confFile);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());

        replaceLocalDir(ret);
        return ret;
    }

    public static boolean isConfigChanged(Map oldConf, Map newConf) {
        if (oldConf.size() != newConf.size()) {
            return true;
        }

        TreeMap sortedOld = new TreeMap(oldConf);
        TreeMap sortedNew = new TreeMap(newConf);
        return !to_json(sortedOld).equals(to_json(sortedNew));
    }

    private static Object normalizeConf(Object conf) {
        if (conf == null)
            return new HashMap();
        if (conf instanceof Map) {
            Map confMap = new HashMap((Map) conf);
            for (Object key : confMap.keySet()) {
                Object val = confMap.get(key);
                confMap.put(key, normalizeConf(val));
            }
            return confMap;
        } else if (conf instanceof List) {
            List confList = new ArrayList((List) conf);
            for (int i = 0; i < confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if (conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }

    public static boolean isValidConf(Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(normalizeConf(Utils.from_json(Utils.to_json(stormConf))));
    }

    public static Object getSetComponentObject(ComponentObject obj, URLClassLoader loader) {
        if (obj.getSetField() == ComponentObject._Fields.SERIALIZED_JAVA) {
            return javaDeserializeWithCL(obj.get_serialized_java(), loader);
        } else if (obj.getSetField() == ComponentObject._Fields.JAVA_OBJECT) {
            return obj.get_java_object();
        } else {
            return obj.get_shell();
        }
    }

    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }

    public static void downloadFromMaster(Map conf, String file, String localFile) throws IOException, TException {
        WritableByteChannel out = null;
        NimbusClient client = null;
        try {
            client = NimbusClient.getConfiguredClient(conf, 10 * 1000);
            String id = client.getClient().beginFileDownload(file);
            out = Channels.newChannel(new FileOutputStream(localFile));
            while (true) {
                ByteBuffer chunk = client.getClient().downloadChunk(id);
                int written = out.write(chunk);
                if (written == 0) {
                    client.getClient().finishFileDownload(id);
                    break;
                }
            }
        } finally {
            if (out != null)
                out.close();
            if (client != null)
                client.close();
        }
    }

    public static IFn loadClojureFn(String namespace, String name) {
        try {
            clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (Exception ignored) {
            // if playing from the repl and defining functions, file won't exist
        }
        return (IFn) RT.var(namespace, name).deref();
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    public static <K, V> Map<V, K> reverseMap(Map<K, V> map) {
        Map<V, K> ret = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            ret.put(entry.getValue(), entry.getKey());
        }
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String id) {
        if (topology.get_spouts().containsKey(id)) {
            return topology.get_spouts().get(id).get_common();
        }
        if (topology.get_bolts().containsKey(id)) {
            return topology.get_bolts().get(id).get_common();
        }
        if (topology.get_state_spouts().containsKey(id)) {
            return topology.get_state_spouts().get(id).get_common();
        }
        throw new IllegalArgumentException("Could not find component with id " + id);
    }

    public static List<String> getStrings(final Object o) {
        if (o == null) {
            return new ArrayList<>();
        } else if (o instanceof String) {
            return Lists.newArrayList((String) o);
        } else if (o instanceof Collection) {
            List<String> answer = new ArrayList<>();
            for (Object v : (Collection) o) {
                answer.add(v.toString());
            }
            return answer;
        } else {
            throw new IllegalArgumentException("Don't know how to convert to string list");
        }
    }

    public static String getString(Object o) {
        if (null == o) {
            throw new IllegalArgumentException("Don't know how to convert null to String");
        }
        return o.toString();
    }

    public static Integer getInt(Object o) {
        Integer result = getInt(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    private static TDeserializer getDes() {
        TDeserializer des = threadDes.get();
        if (des == null) {
            des = new TDeserializer();
            threadDes.set(des);
        }
        return des;
    }

    public static byte[] thriftSerialize(TBase t) {
        try {
            TSerializer ser = threadSer.get();
            if (ser == null) {
                ser = new TSerializer();
                threadSer.set(ser);
            }
            return ser.serialize(t);
        } catch (TException e) {
            LOG.error("Failed to serialize to thrift: ", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class c, byte[] b) {
        try {
            return Utils.thriftDeserialize(c, b, 0, b.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }

        if (o instanceof Integer ||
                o instanceof Short ||
                o instanceof Byte) {
            return ((Number) o).intValue();
        } else if (o instanceof Long) {
            final long l = (Long) o;
            if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
                return (int) l;
            }
        } else if (o instanceof Double) {
            final double d = (Double) o;
            if (Integer.MIN_VALUE <= d && d <= Integer.MAX_VALUE) {
                return (int) d;
            }
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        }

        //
        return defaultValue;
    }

    public static Double getDouble(Object o) {
        Double result = getDouble(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to double");
        }
        return result;
    }

    public static Double getDouble(Object o, Double defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to double");
        }
    }

    public static boolean getBoolean(Object o, boolean defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to boolean");
        }
    }

    public static String getString(Object o, String defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof String) {
            return (String) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to String");
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    /*
     * Unpack matching files from a jar. Entries inside the jar that do
     * not match the given pattern will be skipped.
     *
     * @param jarFile the .jar file to unpack
     * @param toDir the destination directory into which to unpack the jar
     */
    public static void unJar(File jarFile, File toDir)
            throws IOException {
        JarFile jar = new JarFile(jarFile);
        try {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = jar.getInputStream(entry);
                    try {
                        File file = new File(toDir, entry.getName());
                        ensureDirectory(file.getParentFile());
                        OutputStream out = new FileOutputStream(file);
                        try {
                            copyBytes(in, out, 8192);
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            jar.close();
        }
    }

    /**
     * Copies from one stream to another.
     *
     * @param in       InputStream to read from
     * @param out      OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte buf[] = new byte[buffSize];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
        }
    }

    /**
     * Ensure the existence of a given directory.
     *
     * @throws IOException if it cannot be created and does not already exist
     */
    private static void ensureDirectory(File dir) throws IOException {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " +
                    dir.toString());
        }
    }

    /**
     * Given a Tar File as input it will untar the file in a the untar directory
     * passed as the second parameter
     * <p/>
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     * @param inFile   The tar file as input.
     * @param targetDir The untar directory where to untar the tar file.
     * @throws IOException
     */
    public static void unTar(File inFile, File targetDir) throws IOException {
        if (!targetDir.mkdirs()) {
            if (!targetDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + targetDir);
            }
        }

        boolean gzipped = inFile.toString().endsWith("gz");
        if (onWindows()) {
            // Tar is not native to Windows. Use simple Java based implementation for
            // tests and simple tar archives
            unTarUsingJava(inFile, targetDir, gzipped);
        } else {
            // spawn tar utility to untar archive for full fledged unix behavior such
            // as resolving symlinks in tar archives
            unTarUsingTar(inFile, targetDir, gzipped);
        }
    }

    private static void unTarUsingTar(File inFile, File targetDir,
                                      boolean gzipped) throws IOException {
        StringBuilder untarCommand = new StringBuilder();
        if (gzipped) {
            untarCommand.append(" gzip -dc '");
            untarCommand.append(inFile.toString());
            untarCommand.append("' | (");
        }
        untarCommand.append("cd '");
        untarCommand.append(targetDir.toString());
        untarCommand.append("' ; ");
        untarCommand.append("tar -xf ");

        if (gzipped) {
            untarCommand.append(" -)");
        } else {
            untarCommand.append(inFile.toString());
        }
        String[] shellCmd = {"bash", "-c", untarCommand.toString()};
        ShellUtils.ShellCommandExecutor shexec = new ShellUtils.ShellCommandExecutor(shellCmd);
        shexec.execute();
        int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile +
                    ". Tar process exited with exit code " + exitcode);
        }
    }

    private static void unTarUsingJava(File inFile, File targetDir,
                                       boolean gzipped) throws IOException {
        InputStream inputStream = null;
        TarArchiveInputStream tis = null;
        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(
                        new FileInputStream(inFile)));
            } else {
                inputStream = new BufferedInputStream(new FileInputStream(inFile));
            }
            tis = new TarArchiveInputStream(inputStream);
            for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null; ) {
                unpackEntries(tis, entry, targetDir);
                entry = tis.getNextTarEntry();
            }
        } finally {
            cleanup(tis, inputStream);
        }
    }

    /**
     * Close the Closeable objects and <b>ignore</b> any {@link IOException} or
     * null pointers. Must only be used for cleanup in exception handlers.
     *
     * @param closeables the objects to close
     */
    private static void cleanup(java.io.Closeable... closeables) {
        for (java.io.Closeable c : closeables) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    LOG.debug("Exception in closing " + c, e);

                }
            }
        }
    }

    private static void unpackEntries(TarArchiveInputStream tis,
                                      TarArchiveEntry entry, File outputDir) throws IOException {
        if (entry.isDirectory()) {
            File subDir = new File(outputDir, entry.getName());
            if (!subDir.mkdirs() && !subDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                        + outputDir);
            }
            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, subDir);
            }
            return;
        }
        File outputFile = new File(outputDir, entry.getName());
        if (!outputFile.getParentFile().exists()) {
            if (!outputFile.getParentFile().mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                        + outputDir);
            }
        }
        int count;
        byte data[] = new byte[2048];
        BufferedOutputStream outputStream = new BufferedOutputStream(
                new FileOutputStream(outputFile));

        while ((count = tis.read(data)) != -1) {
            outputStream.write(data, 0, count);
        }
        outputStream.flush();
        outputStream.close();
    }

    public static boolean onWindows() {
        return System.getenv("OS") != null && System.getenv("OS").equals("Windows_NT");
    }

    public static void unpack(File src, File dst) throws IOException {
        String lowerDst = src.getName().toLowerCase();
        if (lowerDst.endsWith(".jar")) {
            unJar(src, dst);
        } else if (lowerDst.endsWith(".zip")) {
            unZip(src, dst);
        } else if (lowerDst.endsWith(".tar.gz") ||
                lowerDst.endsWith(".tgz") ||
                lowerDst.endsWith(".tar")) {
            unTar(src, dst);
        } else {
            LOG.warn("Cannot unpack " + src);
            if (!src.renameTo(dst)) {
                throw new IOException("Unable to rename file: [" + src
                        + "] to [" + dst + "]");
            }
        }
        if (src.isFile()) {
            src.delete();
        }
    }


    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root) {
        return newCurator(conf, servers, port, root, null);
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        List<String> serverPorts = new ArrayList<>();
        for (String zkServer : servers) {
            serverPorts.add(zkServer + ":" + Utils.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        setupBuilder(builder, zkStr, conf, auth);

        return builder.build();
    }

    protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, String zkStr, Map conf, ZookeeperAuthInfo auth) {
        builder.connectString(zkStr)
                .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
                .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
                .retryPolicy(
                        new StormBoundedExponentialBackoffRetry(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)), Utils.getInt(conf
                                .get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)), Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

        if (auth != null && auth.scheme != null && auth.payload != null) {
            builder = builder.authorization(auth.scheme, auth.payload);
        }
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        return newCurator(conf, servers, port, "", auth);
    }

    public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, root, auth);
        ret.start();
        return ret;
    }

    public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, auth);
        ret.start();
        return ret;
    }

    /**
     * (defn integer-divided [sum num-pieces] (let [base (int (/ sum num-pieces)) num-inc (mod sum num-pieces) num-bases (- num-pieces num-inc)] (if (= num-inc
     * 0) {base num-bases} {base num-bases (inc base) num-inc} )))
     */

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<>();
        ret.put(base, numBases);
        if (numInc != 0) {
            ret.put(base + 1, numInc);
        }
        return ret;
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    public static void readAndLogStream(String prefix, InputStream in) {
        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = r.readLine()) != null) {
                LOG.info("{}:{}", prefix, line);
            }
        } catch (IOException e) {
            LOG.warn("Error whiel trying to log stream", e);
        }
    }

    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        Throwable t = throwable;
        while (t != null) {
            if (klass.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way? This only works when called from within Nimbus or a Supervisor process.
     *
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map conf) {
        return null != System.getProperty("java.security.auth.login.config")
                || (conf != null && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null &&
                !((String) conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     *
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map conf) {
        return (conf != null && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null &&
                !((String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }

    public static List<ACL> getWorkerACL(Map conf) {
        // This is a work around to an issue with ZK where a sasl super user is not super unless there is an open SASL ACL
        // so we are trying to give the correct perms
        if (!isZkAuthenticationConfiguredTopology(conf)) {
            return null;
        }
        String stormZKUser = (String) conf.get(Config.STORM_ZOOKEEPER_SUPERACL);
        if (stormZKUser == null) {
            throw new IllegalArgumentException("Authentication is enabled but " + Config.STORM_ZOOKEEPER_SUPERACL + " is not set");
        }
        String[] split = stormZKUser.split(":", 2);
        if (split.length != 2) {
            throw new IllegalArgumentException(Config.STORM_ZOOKEEPER_SUPERACL +
                    " does not appear to be in the form scheme:acl, i.e. sasl:storm-user");
        }
        ArrayList<ACL> ret = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
        ret.add(new ACL(ZooDefs.Perms.ALL, new Id(split[0], split[1])));
        return ret;
    }

    public static String threadDump() {
        final StringBuilder dump = new StringBuilder();
        final java.lang.management.ThreadMXBean threadMXBean = java.lang.management.ManagementFactory.getThreadMXBean();
        final java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (java.lang.management.ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    // Assumes caller is synchronizing
    private static SerializationDelegate getSerializationDelegate(Map stormConf) {
        String delegateClassName = (String) stormConf.get(Config.STORM_META_SERIALIZATION_DELEGATE);
        SerializationDelegate delegate;
        try {
            Class delegateClass = Class.forName(delegateClassName);
            delegate = (SerializationDelegate) delegateClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        }
        delegate.prepare(stormConf);
        return delegate;
    }

    public static void handleUncaughtException(Throwable t) {
        if (t != null && t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                try {
                    System.err.println("Halting due to Out Of Memory Error..." + Thread.currentThread().getName());
                } catch (Throwable err) {
                    // Again we don't want to exit because of logging issues.
                }
                Runtime.getRuntime().halt(-1);
            } else {
                // Running in daemon mode, we would pass Error to calling thread.
                throw (Error) t;
            }
        }
    }

    public static List<String> tokenize_path(String path) {
        String[] tokens = path.split("/");
        java.util.ArrayList<String> rtn = new ArrayList<>();
        for (String str : tokens) {
            if (!str.isEmpty()) {
                rtn.add(str);
            }
        }
        return rtn;
    }

    public static String toks_to_path(List<String> tokens) {
        StringBuilder buff = new StringBuilder();
        buff.append("/");
        int size = tokens.size();
        for (int i = 0; i < size; i++) {
            buff.append(tokens.get(i));
            if (i < (size - 1)) {
                buff.append("/");
            }

        }
        return buff.toString();
    }

    public static String normalize_path(String path) {
        return toks_to_path(tokenize_path(path));
    }

    public static String printStack() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nCurrent call stack:\n");
        StackTraceElement[] stackElements = Thread.currentThread().getStackTrace();
        for (int i = 2; i < stackElements.length; i++) {
            sb.append("\t").append(stackElements[i]).append("\n");
        }

        return sb.toString();
    }

    private static Map loadProperty(String prop) {
        Map ret = new HashMap<>();
        Properties properties = new Properties();

        try {
            InputStream stream = new FileInputStream(prop);
            properties.load(stream);
            if (properties.size() == 0) {
                System.out.println("WARN: Config file is empty");
                return null;
            } else {
                ret.putAll(properties);
            }
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + prop);
            throw new RuntimeException(e.getMessage());
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException(e1.getMessage());
        }

        return ret;
    }

    private static Map loadYaml(String confPath) {
        Map ret = new HashMap<>();
        Yaml yaml = new Yaml();
        InputStream stream = null;
        try {
            stream = new FileInputStream(confPath);
            ret = (Map) yaml.load(stream);
            if (ret == null || ret.isEmpty()) {
                System.out.println("WARN: Config file is empty");
                return null;
            }
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + confPath);
            throw new RuntimeException("No config file");
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to read config file");
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return ret;
    }

    public static Map loadConf(String arg) {
        Map ret;
        if (arg.endsWith("yaml")) {
            ret = loadYaml(arg);
        } else {
            ret = loadProperty(arg);
        }
        return ret;
    }

    public static String getVersion() {
        String ret = "";
        InputStream input = null;
        try {
            input = Thread.currentThread().getContextClassLoader().getResourceAsStream("version");
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            String s = in.readLine();
            if (s != null) {
                ret = s.trim();
            } else {
                LOG.warn("Failed to get version");
            }

        } catch (Exception e) {
            LOG.warn("Failed to get version", e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception e) {
                    LOG.error("Failed to close the reader of RELEASE", e);
                }
            }
        }

        return ret;
    }

    public static String getBuildTime() {
        String ret = "";
        InputStream input = null;
        try {
            input = Thread.currentThread().getContextClassLoader().getResourceAsStream("build");
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            String s = in.readLine();
            if (s != null) {
                ret = s.trim();
            } else {
                LOG.warn("Failed to get build time");
            }

        } catch (Exception e) {
            LOG.warn("Failed to get build time", e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception ignored) {
                }
            }
        }

        return ret;
    }

    public static void writeIntToByteArray(byte[] bytes, int offset, int value) {
        bytes[offset++] = (byte) (value & 0x000000FF);
        bytes[offset++] = (byte) ((value & 0x0000FF00) >> 8);
        bytes[offset++] = (byte) ((value & 0x00FF0000) >> 16);
        bytes[offset] = (byte) ((value & 0xFF000000) >> 24);
    }

    public static int readIntFromByteArray(byte[] bytes, int offset) {
        int ret = 0;
        ret = ret | (bytes[offset++] & 0x000000FF);
        ret = ret | ((bytes[offset++] << 8) & 0x0000FF00);
        ret = ret | ((bytes[offset++] << 16) & 0x00FF0000);
        ret = ret | ((bytes[offset] << 24) & 0xFF000000);
        return ret;
    }

    /*
     * Given a File input it will unzip the file in a the unzip directory
     * passed as the second parameter
     * @param inFile The zip file as input
     * @param unzipDir The unzip directory where to unzip the zip file.
     * @throws IOException
     */
    public static void unZip(File inFile, File unzipDir) throws IOException {
        Enumeration<? extends ZipEntry> entries;
        ZipFile zipFile = new ZipFile(inFile);

        try {
            entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = zipFile.getInputStream(entry);
                    try {
                        File file = new File(unzipDir, entry.getName());
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
                        in.close();
                    }
                }
            }
        } finally {
            zipFile.close();
        }
    }

    /**
     * Given a zip File input it will return its size
     * Only works for zip files whose uncompressed size is less than 4 GB,
     * otherwise returns the size module 2^32, per gzip specifications
     *
     * @param myFile The zip file as input
     * @return zip file size as a long
     * @throws IOException
     */
    public static long zipFileSize(File myFile) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(myFile, "r");
        raf.seek(raf.length() - 4);
        long b4 = raf.read();
        long b3 = raf.read();
        long b2 = raf.read();
        long b1 = raf.read();
        long val = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;
        raf.close();
        return val;
    }

    public static double zeroIfNaNOrInf(double x) {
        return (Double.isNaN(x) || Double.isInfinite(x)) ? 0.0 : x;
    }

    /**
     * parses the arguments to extract jvm heap memory size in MB.
     *
     * @return the value of the JVM heap memory setting (in MB) in a java command.
     */
    public static Double parseJvmHeapMemByChildOpts(String input, Double defaultValue) {
        if (input != null) {
            Pattern optsPattern = Pattern.compile("Xmx[0-9]+[mkgMKG]");
            Matcher m = optsPattern.matcher(input);
            String memoryOpts = null;
            while (m.find()) {
                memoryOpts = m.group();
            }
            if (memoryOpts != null) {
                int unit = 1;
                if (memoryOpts.toLowerCase().endsWith("k")) {
                    unit = 1024;
                } else if (memoryOpts.toLowerCase().endsWith("m")) {
                    unit = 1024 * 1024;
                } else if (memoryOpts.toLowerCase().endsWith("g")) {
                    unit = 1024 * 1024 * 1024;
                }
                memoryOpts = memoryOpts.replaceAll("[a-zA-Z]", "");
                Double result = Double.parseDouble(memoryOpts) * unit / 1024.0 / 1024.0;
                return (result < 1.0) ? 1.0 : result;
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static void setClassLoaderForJavaDeSerialize(ClassLoader cl) {
        Utils.cl = cl;
    }

    public static void resetClassLoaderForJavaDeSerialize() {
        Utils.cl = ClassLoader.getSystemClassLoader();
    }

    public static boolean flushToFile(String file, String data, boolean append) {
        if (data == null) {
            return true;
        }
        try {
            FileOutputStream fs = new FileOutputStream(file, append);
            fs.write(data.getBytes());
            fs.flush();
            fs.close();
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
