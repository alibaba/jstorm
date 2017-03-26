package com.alibaba.jstorm.yarn.container;

import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.model.ExecutorMeta;
import com.alibaba.jstorm.yarn.model.STARTType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

/**
 * Created by fengjian on 16/4/18.
 * the executor will launch in and manage real worker for operations
 */
public class Executor {
    private static final Log LOG = LogFactory.getLog(Executor.class);
    private ExecutorMeta executorMeta;
    private String startType = JOYConstants.SUPERVISOR;
    private String logDir = JOYConstants.EMPTY;
    private YarnConfiguration conf;
    private RegistryOperations registryOperations;

    public Executor(String instancName, String shellCommand, STARTType startType, String runningContainer,
                    String localDir, String deployPath,
                    String hadoopHome, String javaHome, String pythonHome, String dstPath, String portList, String shellArgs,
                    String ExecShellStringPath, String applicationId, String supervisorLogviewPort, String nimbusThriftPort) {
        executorMeta = new ExecutorMeta(instancName, shellCommand, startType, runningContainer,
                localDir, deployPath, hadoopHome, javaHome, pythonHome, dstPath, portList, shellArgs,
                ExecShellStringPath, applicationId, supervisorLogviewPort, nimbusThriftPort);

        conf = new YarnConfiguration();
        Path yarnSite = new Path(hadoopHome + JOYConstants.YARN_SITE_PATH);
        conf.addResource(yarnSite);

        //get first log dir
        logDir = conf.get(JOYConstants.YARN_NM_LOG, JOYConstants.YARN_NM_LOG_DIR).split(JOYConstants.COMMA)[0] + JOYConstants.BACKLASH + applicationId + JOYConstants.BACKLASH + runningContainer;
        //Setup RegistryOperations
        registryOperations = RegistryOperationsFactory.createInstance(JOYConstants.YARN_REGISTRY, conf);
        try {
            setupInitialRegistryPaths();
        } catch (IOException e) {
            e.printStackTrace();
        }
        registryOperations.start();
    }

    /**
     * check supervisor's heartBeat,
     *
     * @return
     */
    public boolean checkHeartBeat() {

        String dataPath = executorMeta.getLocalDir();
        File localstate = new File(dataPath + "/data/" + startType + "/" + startType + ".heartbeat/");
        Long modefyTime = localstate.lastModified();

        if (System.currentTimeMillis() - modefyTime > JOYConstants.EXECUTOR_HEARTBEAT_TIMEOUT) {

            LOG.info("----------------------");
            modefyTime = localstate.lastModified();
            LOG.info(modefyTime.toString());

            LOG.info(Long.toString(new Date().getTime()));
            LOG.info(dataPath + "/data/" + startType + "/" + startType + ".heartbeat/");

            LOG.info("can't get heartbeat over " + String.valueOf(JOYConstants.EXECUTOR_HEARTBEAT_TIMEOUT) + " seconds");
            return false;
        } else
            return true;
    }

    /**
     * TODO: purge this once RM is doing the work
     *
     * @throws IOException
     */
    protected void setupInitialRegistryPaths() throws IOException {
        if (registryOperations instanceof RMRegistryOperationsService) {
            RMRegistryOperationsService rmRegOperations =
                    (RMRegistryOperationsService) registryOperations;
            rmRegOperations.initUserRegistryAsync(RegistryUtils.currentUser());
        }
    }

    public boolean checkProcess(String pid) {
        boolean exist = true;
        try {
            String line;
            Process p = Runtime.getRuntime().exec("kill -0 " + pid);
            BufferedReader input =
                    new BufferedReader
                            (new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                if (line.contains("fail"))
                    exist = false;
                LOG.info("process read :" + line.toString());
            }
            input.close();
        } catch (Exception err) {
            err.printStackTrace();
        }
        return exist;
    }

    public String getPid() {
        String dataPath = executorMeta.getLocalDir();
        File pids = new File(dataPath + "/data/" + startType + "/pids/");
        if (pids.listFiles() == null || pids.listFiles().length == 0) {
            LOG.error("cant get pid of " + dataPath + "/data/" + startType + "/pids/");
            return null;
        }
        File pidfile = pids.listFiles()[0];
        String pid = pidfile.getName();
        LOG.info("get pid:" + pid);
        return pid;
    }

    public void killProcess() {
        LOG.info("start to kill");

        String pid = getPid();
        if (checkProcess(pid)) {
            LOG.info("process :" + pid + " exist");
        } else {
            LOG.info("process :" + pid + " not exist");
        }
        try {
            Runtime.getRuntime().exec("kill " + pid);
            Thread.sleep(3000);
            Runtime.getRuntime().exec("kill -9 " + pid);
            LOG.info("kill pid " + pid);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("kill pid failed " + pid, e);
        }
    }

    /**
     * reload jstorm file
     *
     * @return
     */
    public boolean upgradeJstorm() {
        killProcess();
        return false;
    }

    public boolean needUpgrade() {
        String containerPath = RegistryUtils.componentPath(
                JOYConstants.APP_TYPE, this.executorMeta.getInstanceName(),
                this.executorMeta.getApplicationId(), this.executorMeta.getRunningContainer());

        try {
            if (registryOperations.exists(containerPath)) {
                ServiceRecord sr = registryOperations.resolve(containerPath);
                if (sr.get(JOYConstants.NEED_UPGRADE) != null && sr.get(JOYConstants.NEED_UPGRADE).equals(JOYConstants.TRUE)) {
                    sr.set(JOYConstants.NEED_UPGRADE, JOYConstants.FALSE);
                    registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                    LOG.info(JOYConstants.NEED_UPGRADE);
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void run() {
        // Set the necessary command to execute on the allocated container
        Vector<CharSequence> vargs = new Vector<CharSequence>(9);
        vargs.add(executorMeta.getShellCommand());
        vargs.add(executorMeta.getExecShellStringPath());

        // start type specified to be excute by shell script to start jstorm process
        if (executorMeta.getStartType() == STARTType.NIMBUS) {
            startType = JOYConstants.NIMBUS;
        } else {
            startType = JOYConstants.SUPERVISOR;
        }
        vargs.add(startType);
        vargs.add(executorMeta.getLocalDir());
        vargs.add(executorMeta.getDeployPath());
        String portListStr = executorMeta.getPortList();
        vargs.add(portListStr);
        String hadoopHome = executorMeta.getHadoopHome();
        String javaHome = executorMeta.getJavaHome();
        String pythonHome = executorMeta.getPythonHome();
        vargs.add(hadoopHome);
        vargs.add(javaHome);//$6
        vargs.add(pythonHome);//$7

        String deployDst = executorMeta.getDstPath();
        String dstPath = deployDst;
        vargs.add(dstPath);//$8
        String supervisorLogviewPort = executorMeta.getSupervisorLogviewPort();
        vargs.add(supervisorLogviewPort);//$9

        String nimbusThriftPort = executorMeta.getNimbusThriftPort();
        vargs.add(nimbusThriftPort);//$10
        vargs.add(logDir + "/stderr");//$11
        vargs.add(executorMeta.getShellArgs());
        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(JOYConstants.BLANK);
        }

        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        LOG.info("container command is :" + command.toString());
        try {
            LOG.info("start process");
            //set system env
            LOG.info(JOYConstants.CONTAINER_SUPERVISOR_HEARTBEAT + JOYConstants.EQUAL + executorMeta.getLocalDir() + JOYConstants.JSTORM_SUPERVISOR_HEARTBEAT_PATH);
            Process p = Runtime.getRuntime().exec(command.toString(),
                    new String[]{JOYConstants.CONTAINER_SUPERVISOR_HEARTBEAT + JOYConstants.EQUAL + executorMeta.getLocalDir() + JOYConstants.JSTORM_SUPERVISOR_HEARTBEAT_PATH,
                            JOYConstants.CONTAINER_NIMBUS_HEARTBEAT + JOYConstants.EQUAL + executorMeta.getLocalDir() + JOYConstants.JSTORM_NIMBUS_HEARTBEAT_PATH});
            //wait 30 seconds
            Thread.sleep(JOYConstants.EXECUTOR_LOOP_TIME);
        } catch (Exception err) {
            err.printStackTrace();
        }
        LOG.info("the executor is running");

    }

    public void waitRunning() {
        while (true) {
            try {
                if (checkHeartBeat() && !needUpgrade()) {
                    Thread.sleep(3000);
                } else {
                    killProcess();
                    LOG.error(startType + " 's process is dead");
                    run();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * set local conf
     *
     * @param key
     * @param value
     * @return
     */
    public boolean setJstormConf(String key, String value) {
        //todo: how to set conf from a signal
        // could pull configuration file from nimbus
        String line = " " + key + ": " + value;
        try {
            Files.write(Paths.get("deploy/jstorm/conf/storm.yaml"), line.getBytes(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            LOG.error(e);
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        boolean result = false;
        String instanceName = args[0];
        String shellCommand = args[1];
        String startType = args[2];
        String runningcontainerId = args[3];
        String localDir = args[4];
        String deployPath = args[5];
        String hadoopHome = args[6];
        String javaHome = args[7];
        String pythonHome = args[8];
        String dstPath = args[9];
        String portList = args[10];
        String ExecShellStringPath = args[11];
        String applicationId = args[12];
        String supervisorLogView = args[13];
        String nimbusThriftPort = args[14];
        String shellArgs = "";
        if (args.length > 14)
            shellArgs = args[14];

        try {
            Executor executor = new Executor(instanceName, shellCommand, startType.equals(JOYConstants.NIMBUS) ? STARTType.NIMBUS : STARTType.SUPERVISOR,
                    runningcontainerId,
                    localDir, deployPath, hadoopHome, javaHome, pythonHome, dstPath, portList, shellArgs, ExecShellStringPath, applicationId, supervisorLogView, nimbusThriftPort);
            executor.run();
            executor.waitRunning();
            LOG.info("Initializing Client");
        } catch (Throwable t) {
            LOG.fatal("Error running Executor", t);
            System.exit(JOYConstants.EXIT_FAIL);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(JOYConstants.EXIT_SUCCESS);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(JOYConstants.EXIT_FAIL);
    }
}
