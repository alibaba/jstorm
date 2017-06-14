/*
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
package com.alibaba.jstorm.dfs;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface for distributed file system, e.g. HDFS
 */
public interface IDfs {
    public String getBaseDir();

    public void create(String dstPath, byte[] data) throws IOException;

    public byte[] read(String dstPath) throws IOException;

    public Collection<String> readLines(String dstPath) throws IOException;

    public boolean exist(String path) throws IOException;

    public void mkdir(String dstDir) throws IOException;

    public boolean remove(String dstPath, boolean recursive) throws IOException;

    public void copyToDfs(String srcPath, String dstPath, boolean overwrite) throws IOException;

    public void copyToLocal(String srcPath, String dstPath) throws IOException;

    public Collection<String> listFile(String dstPath, boolean recursive) throws IOException;

    public void close();
}