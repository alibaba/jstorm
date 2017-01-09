package com.alibaba.jstorm.yarn.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import ch.ethz.ssh2.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by fengjian on 16/1/6.
 */
public class SSHOperations {
    private String srcHost;
    private String dstHost;
    private String amHost;
    private String user;
    private String password;
    private String oldPassword;

    private static final Log LOG = LogFactory.getLog(SSHOperations.class);

    public SSHOperations(String srcHost, String dstHost, String amHost, String user, String password, String oldPassword) {
        this.srcHost = srcHost;
        this.dstHost = dstHost;
        this.amHost = amHost;
        this.user = user;
        this.password = password;
        this.oldPassword = oldPassword;
    }

    /**
     * get remoteFile from specific srcHost and directory
     *
     * @return true or false
     */
    public boolean scpGet(String remoteFile, String localDir, String dst, String instanceName) {
        LOG.info("remoteFile:" + remoteFile + " localdir:" + localDir +
                " dst:" + dst + " instanceName:" + instanceName);
        LOG.info("srchost:" + srcHost + "  dsthost:" + dstHost + "  amHost:" + amHost);

        ScpFrom scpFrom = new ScpFrom(srcHost, user, password);
        ScpTo scpTo = new ScpTo(dstHost, user, password);
        Exec exec = new Exec(srcHost, user, password);

        try {
            String execCommand = "cd " + remoteFile + " && cd .. && tar -cf " + instanceName + ".tar "
                    + instanceName;
            LOG.info("execCommand1:" + execCommand);
            exec.execute(execCommand);
            LOG.info("exec success");
        } catch (Exception ex) {
            try {
                exec = new Exec(srcHost, user, oldPassword);
                String execCommand = "cd " + remoteFile + " && cd .. && tar -cf " + instanceName + ".tar "
                        + instanceName;
                LOG.info("execCommand1:" + execCommand);
                exec.execute(execCommand);
                LOG.info("exec success");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + password);
                return false;
            }
        }

        try {
            //
            if (!amHost.split("/")[1].equals(srcHost) && !amHost.split("/")[0].equals(srcHost)) {
                scpFrom.getRemoteFile(remoteFile + ".tar", localDir);
                LOG.info("getRemoteFile:" + remoteFile + ".tar");
            }
        } catch (Exception ex) {
            try {
                scpFrom = new ScpFrom(srcHost, user, oldPassword);
                scpFrom.getRemoteFile(remoteFile + ".tar", localDir);
                LOG.info("getRemoteFile:" + remoteFile + ".tar");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + password);
                return false;
            }
        }

        try {
            if (!amHost.split("/")[1].equals(dstHost) && !amHost.split("/")[0].equals(dstHost)) {
                //put file to remote
                scpTo.putFile(localDir + instanceName + ".tar", localDir);
                LOG.info("putFileToRemote:" + remoteFile + ".tar");
            }
        } catch (Exception ex) {
            try {
                scpTo = new ScpTo(srcHost, user, oldPassword);
                scpTo.putFile(remoteFile + ".tar", localDir);
                LOG.info("putFileToRemote:" + remoteFile + ".tar");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + oldPassword);
                return false;
            }
        }

        try {
            Exec execRemote = new Exec(dstHost, user, password);
            execRemote.execute("cd " + localDir + "  &&  tar -xf " + instanceName + ".tar");
        } catch (Exception ex) {
            try {
                Exec execRemote = new Exec(dstHost, user, oldPassword);
                execRemote.execute("cd " + localDir + "  && tar -xf " + instanceName + ".tar");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + oldPassword);
                return false;
            }
        }

        return true;
    }

    public boolean mkdir(String dataPath, String containerId) {

        Exec exec = new Exec(dstHost, user, password);
        String path = dataPath + containerId;
        try {
            String execCommand = "mkdir " + path;
            LOG.info("execCommand1:" + execCommand);
            exec.execute(execCommand);
            LOG.info("exec success");
        } catch (Exception ex) {
            try {
                exec = new Exec(dstHost, user, oldPassword);
                String execCommand = "mkdir " + path;
                LOG.info("execCommand1:" + execCommand);
                exec.execute(execCommand);
                LOG.info("exec success");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + password);
                return false;
            }
        }
        return true;
    }

    /**
     * get remoteFile from specific srcHost and directory
     *
     * @return true or false
     */
    public boolean transferNimbusdata(String dataPath,String preDataPath, String preContainerId, String containerId, String instanceName) {
        LOG.info("dataPath:" + dataPath + " preContainerId:" + preContainerId +
                " containerId:" + containerId + " instanceName:" + instanceName);
        LOG.info("srchost:" + srcHost + "  dsthost:" + dstHost + "  amHost:" + amHost);

        ScpFrom scpFrom = new ScpFrom(srcHost, user, password);
        ScpTo scpTo = new ScpTo(dstHost, user, password);
        Exec exec = new Exec(srcHost, user, password);

        try {
            String execCommand = "cd " + preDataPath + preContainerId + "  && tar -cf " + instanceName + ".tar "
                    + instanceName;
            LOG.info("execCommand1:" + execCommand);
            exec.execute(execCommand);
            LOG.info("exec success");
        } catch (Exception ex) {
            try {
                exec = new Exec(srcHost, user, oldPassword);
                String execCommand = "cd " + preDataPath + preContainerId + "  && tar -cf " + instanceName + ".tar "
                        + instanceName;
                LOG.info("execCommand1:" + execCommand);
                exec.execute(execCommand);
                LOG.info("exec success");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + password);
                return false;
            }
        }

        try {
            //
//            if (!amHost.split("/")[1].equals(srcHost) && !amHost.split("/")[0].equals(srcHost)) {
            String remoteFile = preDataPath + preContainerId + "/" + instanceName + ".tar";
            scpFrom.getRemoteFile(remoteFile, dataPath);
            LOG.info("getRemoteFile:" + remoteFile);
//            }
        } catch (Exception ex) {
            try {
                scpFrom = new ScpFrom(srcHost, user, oldPassword);
                String remoteFile = preDataPath + preContainerId + "/" + instanceName + ".tar";
                scpFrom.getRemoteFile(remoteFile, dataPath);
                LOG.info("getRemoteFile:" + remoteFile);
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + password + "  oldpassword:" + oldPassword);
                return false;
            }
        }

        try {
            if (!amHost.split("/")[1].equals(dstHost) && !amHost.split("/")[0].equals(dstHost)) {
                //put file to remote
                String localFile = dataPath + instanceName + ".tar";
                scpTo.putFile(localFile, dataPath);
                LOG.info("putFileToRemote:" + localFile + "  remoteDir:" + dataPath);
            }
        } catch (Exception ex) {
            try {
                if (!amHost.split("/")[1].equals(dstHost) && !amHost.split("/")[0].equals(dstHost)) {
                    scpTo = new ScpTo(dstHost, user, oldPassword);
                    String localFile = dataPath + instanceName + ".tar";
                    scpTo.putFile(localFile, dataPath);
                    LOG.info("putFileToRemote:" + localFile + "  remoteDir:" + dataPath);
                }
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + oldPassword);
                return false;
            }
        }

        try {
            Exec execRemote = new Exec(dstHost, user, password);
            String fileName = instanceName + ".tar";
            LOG.info("cd " + dataPath + " && cp " + fileName + " " + containerId
                    + "/ && cd " + containerId + "/ &&   tar -xf "
                    + instanceName + ".tar");
            execRemote.execute("cd " + dataPath + " && cp " + fileName + " " + containerId
                    + "/ && cd " + containerId + "/ &&   tar -xf "
                    + instanceName + ".tar");
        } catch (Exception ex) {
            try {
                Exec execRemote = new Exec(dstHost, user, oldPassword);
                String fileName = instanceName + ".tar";
                execRemote.execute("cd " + dataPath + " && cp " + fileName + " " + containerId
                        + "/ && cd " + containerId + "/ &&   tar -xf "
                        + instanceName + ".tar");
            } catch (Exception eex) {
                LOG.error(eex);
                LOG.error("username:" + user + "  password:" + oldPassword);
                return false;
            }
        }

        return true;
    }

    public boolean GetPreviousData(String path) {
        String user = "jian.feng";
        String pass = "******";

        Connection con = new Connection(srcHost);
        try {
            con.connect();
//            boolean isAuthed = con.authenticateWithPassword(user, pass);
//            System.out.println("isAuthed====" + isAuthed);


            SCPClient scpClient = con.createSCPClient();
//            scpClient.put("localFiles", "remoteDirectory"); //从本地复制文件到远程目录
            scpClient.get("remoteFiles", "");  //从远程获取文件


//
//            SFTPv3Client sftpClient = new SFTPv3Client(con);
//            sftpClient.mkdir("newRemoteDir", 6);    //远程新建目录
//            sftpClient.rmdir("");                   //远程删除目录
//
//            sftpClient.createFile("newRemoteFile"); //远程新建文件
//            sftpClient.openFileRW("remoteFile");    //远程打开文件，可进行读写

            Session session = con.openSession();
            session.execCommand("uname -a && date && uptime && who");   //远程执行命令


            //显示执行命令后的信息
            System.out.println("Here is some information about the remote srcHost:");
            InputStream stdout = new StreamGobbler(session.getStdout());

            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));

            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                System.out.println(line);
            }

            /* Show exit status, if available (otherwise "null") */

            System.out.println("ExitCode: " + session.getExitStatus());

            session.close();
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
