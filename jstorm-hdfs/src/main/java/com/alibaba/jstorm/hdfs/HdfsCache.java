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
package com.alibaba.jstorm.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.alibaba.jstorm.dfs.IDfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class HdfsCache implements IDfs {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsCache.class);

    private String baseDir = "/user/admin/cluster";

    private Configuration hadoopConf;
    private FileSystem fs;

    public HdfsCache(Map conf) {
        this.hadoopConf = new Configuration();
        String hdfsHostName = (String) conf.get(Config.BLOBSTORE_HDFS_HOSTNAME);
        Integer hdfsPort = JStormUtils.parseInt(conf.get(Config.BLOBSTORE_HDFS_PORT));
        LOG.info("hdfs address hdfs://{}:{}", hdfsHostName, hdfsPort);
        hadoopConf.set("fs.defaultFS", String.format("hdfs://%s:%d", hdfsHostName, hdfsPort));
        hadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true);

        try {
            fs = FileSystem.get(hadoopConf);
            String configBaseDir = (String) conf.get("hdfs.base.dir");
            if (configBaseDir != null) {
                this.baseDir = configBaseDir;
            } else {
                String clusterName = ConfigExtension.getClusterName(conf) != null ? ConfigExtension.getClusterName(conf) : "default";
                baseDir = baseDir + "/" + clusterName;
                if (!exist(baseDir))
                    mkdir(baseDir);
            }
        } catch (IOException e) {
            LOG.error("Failed to instance hdfs cache", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void create(String dstPath, byte[] data) throws IOException {
        Path path = new Path(dstPath);
        LOG.debug("Try to create file: {}", dstPath);
        FSDataOutputStream out = null;
        try {
            out = fs.create(path, true);
            out.write(data);
        } finally {
            if (out != null)
                out.close();
        }
    }

    public byte[] read(String dstPath) throws IOException {
        Path path = new Path(dstPath);
        if (!fs.exists(path)) {
            throw new IOException(dstPath + " is not exist!");
        }

        
        FSDataInputStream in = null;
        try {
            in = fs.open(path);
            LOG.debug("Try to read data from file-{}, dataLen={}", dstPath, in.available());
            ByteBuffer buf = ByteBuffer.allocate(in.available());
            in.read(buf);
            return buf.array();
        } finally {
            if (in != null)
                in.close();
        }
    }

    public Collection<String> readLines(String dstPath) throws IOException {
        Collection<String> ret = new HashSet<String>();
        Path path = new Path(dstPath);
        if (!fs.exists(path)) {
            throw new IOException(dstPath + " is not exist!");
        }

        FSDataInputStream in = null;
        try {
            in = fs.open(path);
            LOG.debug("Try to read string from file-{}", dstPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            while(line != null) {
                ret.add(line);
                line = reader.readLine();
            }
            return ret;
        } finally {
            if (in != null)
                in.close();
        }
    }

    public boolean exist(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    public void mkdir(String dstDir) throws IOException {
        Path path = new Path(dstDir);
        if (!fs.exists(path)) {
            LOG.debug("Try to makdir: {}", dstDir);
            fs.mkdirs(path); // mkdirs -p
        }
    }

    public boolean remove(String dstPath, boolean recursive) throws IOException {
        Path path = new Path(dstPath);
        if (fs.exists(path)) {
            LOG.debug("Try to remove file-{}", dstPath);
            return fs.delete(path, recursive);
        } else {
            return false;
        }
    }

    public void copyToDfs(String srcPath, String dstPath, boolean overwrite) throws IOException {
        Path path = new Path(dstPath);
        if (!fs.exists(path))
            fs.mkdirs(path);
        fs.copyFromLocalFile(false, overwrite, new Path(srcPath), new Path(dstPath));
    }

    public void copyToLocal(String srcPath, String dstPath) throws IOException {
        fs.copyToLocalFile(new Path(srcPath), new Path(dstPath));
    }

    public Collection<String> listFile(String dstPath, boolean recursive) throws IOException {
        Collection<String> files = new HashSet<String>();
        Path path = new Path(dstPath);
        if (fs.exists(path)) {
            RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, recursive);
            while(itr.hasNext()) {
                LocatedFileStatus status = itr.next();
                files.add(status.getPath().getName());
            }
        }
        return files;
    }

    public FileStatus[] listFileStatus(String dstPath) throws IOException {
        FileStatus[] ret = null;
        Path path = new Path(dstPath);
        if (fs.exists(path)) {
            ret = fs.listStatus(path);
        }
        return ret;
    }

    public FileStatus getFileStatus(String dstPath) throws IOException {
        FileStatus ret = null;
        Path path = new Path(dstPath);
        if (fs.exists(path)) {
            ret = fs.getFileStatus(path);
        }
        return ret;
    }

    public void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                LOG.warn("Failed to close hadoop file system!", e);
            }
        }
    }

    public static void main(String[] args) {
    	int argsStartIndex = 0;
    	Map conf = new HashMap<Object, Object>();
    	if (args[0].equals("-conf")) {
    	    conf.putAll(Utils.loadConf(args[1]));
    	    argsStartIndex += 2;
    	} else {
    		System.out.println("-conf is missing!");
    		return;
    	}

        conf.put("hdfs.base.dir", "/tmp");
        conf.put("cluster.name", "test");
        HdfsCache cache = new HdfsCache(conf);
        String topoPath = cache.getBaseDir() + "/topology1";

        String cmd = args[argsStartIndex];
        try {
            if (cmd.equals("create")) {
            	String path = topoPath + "/" + args[++argsStartIndex];
                cache.create(path, "Test".getBytes());
            } else if (cmd.equals("read")) {
                String path = topoPath + "/" + args[++argsStartIndex];
                byte[] data = cache.read(path);
                System.out.println("Read string from hdfs, string=" + new String(data));
            } else if (cmd.equals("mkdir")) {
                String path = topoPath + "/" + args[++argsStartIndex];
                cache.mkdir(path);
            } else if (cmd.equals("remove")) {
                String path = topoPath + "/" + args[++argsStartIndex];
                cache.remove(path, true);
            } else if(cmd.equals("copy")) {
                boolean toHdfs = Boolean.valueOf(args[++argsStartIndex]);
                String srcPath = args[++argsStartIndex];
                String dstPath = args[++argsStartIndex];
                if (toHdfs) {
                    cache.copyToDfs(srcPath, dstPath, false);
                } else {
                    cache.copyToLocal(srcPath, dstPath);
                }
            } else {
            	System.out.println("Unknown command: " + args[argsStartIndex + 0]);
            }
        } catch (IOException e) {
            System.out.println("Failed to execute cmd: " + cmd);
            e.printStackTrace();
        }
    }
}