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
package com.alibaba.jstorm.yarn.utils;/* -*-mode:java; c-basic-offset:2; indent-tabs-mode:nil -*- */

import com.jcraft.jsch.*;

import javax.swing.*;
import java.awt.*;
import java.io.InputStream;
/**
 * Created by fengjian on 16/4/7.
 */
public class Exec {

    public Exec(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
    }

    private String host;
    private String username;
    private String password;

    public void execute(String command) throws Exception {
        JSch jsch = new JSch();

        Session session = jsch.getSession(username, host, 22);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();

        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        channel.setInputStream(null);

//        ((ChannelExec) channel).setErrStream(System.err);

        InputStream in = channel.getInputStream();

        channel.connect();

        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
//                System.out.print(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                if (in.available() > 0) continue;
                if (channel.getExitStatus() != 0)
                    throw new Exception("exitStatus:" + channel.getExitStatus());
//                System.out.println("exit-status: " + channel.getExitStatus());
                break;
            }
//            try {
//                Thread.sleep(100);
//            } catch (Exception ee) {
//            }
        }
        channel.disconnect();
        session.disconnect();
    }

    public static void main(String[] arg) {


        try {
            JSch jsch = new JSch();

            Session session = jsch.getSession("fengjian", "localhost", 22);
            session.setPassword("");
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            String command = "ls";
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            // X Forwarding
            // channel.setXForwarding(true);

            //channel.setInputStream(System.in);
            channel.setInputStream(null);

            //channel.setOutputStream(System.out);

            //FileOutputStream fos=new FileOutputStream("/tmp/stderr");
            //((ChannelExec)channel).setErrStream(fos);
            ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();

            channel.connect();

            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    System.out.print(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) continue;
                    System.out.println("exit-status: " + channel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}