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
package backtype.storm;

import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program with the "storm jar" command from the command-line, and then use
 * this class to submit your topologies.
 */
public class StormSubmitter {
    public static Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);

    private static Nimbus.Iface localNimbus = null;

    public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
        StormSubmitter.localNimbus = localNimbusHandler;
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     * 
     * 
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        submitTopology(name, stormConf, topology, null);
    }

    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts, List<File> jarFiles)
            throws AlreadyAliveException, InvalidTopologyException {
        if (jarFiles == null) {
            jarFiles = new ArrayList<File>();
        }
        Map<String, String> jars = new HashMap<String, String>(jarFiles.size());
        List<String> names = new ArrayList<String>(jarFiles.size());

        for (File f : jarFiles) {
            if (!f.exists()) {
                LOG.info(f.getName() + " is not existed: " + f.getAbsolutePath());
                continue;
            }
            jars.put(f.getName(), f.getAbsolutePath());
            names.add(f.getName());
        }
        LOG.info("Files: " + names + " will be loaded");
        stormConf.put(GenericOptionsParser.TOPOLOGY_LIB_PATH, jars);
        stormConf.put(GenericOptionsParser.TOPOLOGY_LIB_NAME, names);
        submitTopology(name, stormConf, topology, opts);
    }

    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts, ProgressListener listener)
            throws AlreadyAliveException, InvalidTopologyException {
        submitTopology(name, stormConf, topology, opts);
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     * 
     * 
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException,
            InvalidTopologyException {
        if (!Utils.isValidConf(stormConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }
        stormConf = new HashMap(stormConf);
        stormConf.putAll(Utils.readCommandLineOpts());
        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        putUserInfo(conf, stormConf);
        try {
            String serConf = Utils.to_json(stormConf);
            if (localNimbus != null) {
                LOG.info("Submitting topology " + name + " in local mode");
                localNimbus.submitTopology(name, null, serConf, topology);
            } else {
                NimbusClient client = NimbusClient.getConfiguredClient(conf);
                try {
                    boolean enableDeploy = ConfigExtension.getTopologyHotDeplogyEnable(stormConf);

                    if (topologyNameExists(client, conf, name) != enableDeploy) {
                        if (enableDeploy){
                            throw new RuntimeException("Topology with name `" + name + "` not exists on cluster");
                        }else {
                            throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
                        }
                    }

                    submitJar(client, conf);
                    LOG.info("Submitting topology " + name + " in distributed mode with conf " + serConf);
                    if (opts != null) {
                        client.getClient().submitTopologyWithOpts(name, path, serConf, topology, opts);
                    } else {
                        // this is for backwards compatibility
                        client.getClient().submitTopology(name, path, serConf, topology);
                    }
                } finally {
                    client.close();
                }
            }
            LOG.info("Finished submitting topology: " + name);
        } catch (InvalidTopologyException e) {
            LOG.warn("Topology submission exception", e);
            throw e;
        } catch (AlreadyAliveException e) {
            LOG.warn("Topology already alive exception", e);
            throw e;
        } catch (TopologyAssignException e) {
            LOG.warn("Failed to assign " + e.get_msg(), e);
            throw new RuntimeException(e);
        } catch (TException e) {
            LOG.warn("Failed to assign ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until explicitly killed.
     * 
     * 
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws TopologyAssignException
     */

    public static void submitTopologyWithProgressBar(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        submitTopologyWithProgressBar(name, stormConf, topology, null);
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until explicitly killed.
     * 
     * 
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts to manipulate the starting of the topology
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws TopologyAssignException
     */

    public static void submitTopologyWithProgressBar(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException,
            InvalidTopologyException {

        /**
         * remove progress bar in jstorm
         */
        submitTopology(name, stormConf, topology, opts);
    }

    public static boolean topologyNameExists(NimbusClient client, Map conf, String name) {
        try {
            client.getClient().getTopologyInfoByName(name);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String submittedJar = null;
    private static String path = null;

    private static void submitJar(NimbusClient client, Map conf) {
        if (submittedJar == null) {
            try {
                LOG.info("Jar not uploaded to master yet. Submitting jar...");
                String localJar = System.getProperty("storm.jar");
                path = client.getClient().beginFileUpload();
                String[] pathCache = path.split("/");
                String uploadLocation = path + "/stormjar-" + pathCache[pathCache.length - 1] + ".jar";
                List<String> lib = (List<String>) conf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
                Map<String, String> libPath = (Map<String, String>) conf.get(GenericOptionsParser.TOPOLOGY_LIB_PATH);
                if (lib != null && lib.size() != 0) {
                    for (String libName : lib) {
                        String jarPath = path + "/lib/" + libName;
                        client.getClient().beginLibUpload(jarPath);
                        submitJar(conf, libPath.get(libName), jarPath, client);
                    }

                } else {
                    if (localJar == null) {
                        // no lib, no client jar
                        throw new RuntimeException("No client app jar, please upload it");
                    }
                }

                if (localJar != null) {
                    submittedJar = submitJar(conf, localJar, uploadLocation, client);
                } else {
                    // no client jar, but with lib jar
                    client.getClient().finishFileUpload(uploadLocation);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.info("Jar already uploaded to master. Not submitting jar.");
        }
    }

    public static String submitJar(Map conf, String localJar, String uploadLocation, NimbusClient client) {
        if (localJar == null) {
            throw new RuntimeException("Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
        }

        try {

            LOG.info("Uploading topology jar " + localJar + " to assigned location: " + uploadLocation);
            int bufferSize = 512 * 1024;
            Object maxBufSizeObject = conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE);
            if (maxBufSizeObject != null) {
                bufferSize = Utils.getInt(maxBufSizeObject) / 2;
            }

            BufferFileInputStream is = new BufferFileInputStream(localJar, bufferSize);
            while (true) {
                byte[] toSubmit = is.read();
                if (toSubmit.length == 0)
                    break;
                client.getClient().uploadChunk(uploadLocation, ByteBuffer.wrap(toSubmit));
            }
            client.getClient().finishFileUpload(uploadLocation);
            LOG.info("Successfully uploaded topology jar to assigned location: " + uploadLocation);
            return uploadLocation;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

        }
    }

    private static void putUserInfo(Map conf, Map stormConf) {
        stormConf.put("user.group", conf.get("user.group"));
        stormConf.put("user.name", conf.get("user.name"));
        stormConf.put("user.password", conf.get("user.password"));
    }

    /**
     * Interface use to track progress of file upload
     */
    public interface ProgressListener {
        /**
         * called before file is uploaded
         * 
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        public void onStart(String srcFile, String targetFile, long totalBytes);

        /**
         * called whenever a chunk of bytes is uploaded
         * 
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param bytesUploaded - number of bytes transferred so far
         * @param totalBytes - total number of bytes of the file
         */
        public void onProgress(String srcFile, String targetFile, long bytesUploaded, long totalBytes);

        /**
         * called when the file is uploaded
         * 
         * @param srcFile - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        public void onCompleted(String srcFile, String targetFile, long totalBytes);
    }
}