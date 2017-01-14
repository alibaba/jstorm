package com.alibaba.jstorm.yarn.utils;/* -*-mode:java; c-basic-offset:2; indent-tabs-mode:nil -*- */

import com.jcraft.jsch.*;

import javax.swing.*;
import java.awt.*;
import java.io.InputStream;

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
      
      /*
      String xhost="127.0.0.1";
      int xport=0;
      String display=JOptionPane.showInputDialog("Enter display name", 
                                                 xhost+":"+xport);
      xhost=display.substring(0, display.indexOf(':'));
      xport=Integer.parseInt(display.substring(display.indexOf(':')+1));
      session.setX11Host(xhost);
      session.setX11Port(xport+6000);
      */

            // username and password will be given via UserInfo interface.
//            UserInfo ui = new MyUserInfo();
//            session.setUserInfo(ui);
//            session.connect();

//            String command = JOptionPane.showInputDialog("Enter command",
//                    "set|grep SSH");

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