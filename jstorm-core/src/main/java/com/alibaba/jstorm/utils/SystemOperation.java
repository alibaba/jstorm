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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemOperation {

    public static final Logger LOG = LoggerFactory.getLogger(SystemOperation.class);

    public static boolean isRoot() throws IOException {
        String result = SystemOperation.exec("echo $EUID").substring(0, 1);
        return Integer.valueOf(result.substring(0, result.length())).intValue() == 0 ? true : false;
    };

    public static void mount(String name, String target, String type, String data) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("mount -t ").append(type).append(" -o ").append(data).append(" ").append(name).append(" ").append(target);
        SystemOperation.exec(sb.toString());
    }

    public static void umount(String name) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("umount ").append(name);
        SystemOperation.exec(sb.toString());
    }

    public static String exec(String cmd) throws IOException {
        List<String> commands = new ArrayList<String>();
        commands.add("/bin/bash");
        commands.add("-c");
        commands.add(cmd);

        return JStormUtils.launchProcess(cmd, commands, new HashMap<String, String>(), false);
    }

    public static void main(String[] args) throws IOException {
        SystemOperation.mount("test", "/cgroup/cpu", "cgroup", "cpu");
    }

}
