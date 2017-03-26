package backtype.storm.command;

import backtype.storm.utils.ShellUtils;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.MachineCheckStatus;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author dongbin.db
 * @since 2.1.1
 */
public class health {

    private static Logger LOG = LoggerFactory.getLogger(health.class);

    private static final String NO_RESOURCES = "no_resource";

    public static MachineCheckStatus check() {
        MachineCheckStatus status = new MachineCheckStatus();
        Map conf = Utils.readStormConfig();
        // firstly check panic's dir
        status.updatePanic();
        runJStormMachineResourceCheckScript(status, conf);
        if (status.getType() == MachineCheckStatus.StatusType.panic) {
            return status;
        }
        // sencondly check error's dir
        status.updateError();
        runJStormMachineResourceCheckScript(status, conf);
        if (status.getType() == MachineCheckStatus.StatusType.error) {
            return status;
        }
        // thirdly check waring's dir
        status.updateWarning();
        runJStormMachineResourceCheckScript(status, conf);
        return status;
    }

    private static void runJStormMachineResourceCheckScript(MachineCheckStatus status, Map conf) {
        try {
            long timeOut = ConfigExtension.getStormHealthTimeoutMs(conf);
            List<String> evalScripts = null;
            if (status.getType().equals(MachineCheckStatus.StatusType.panic)) {
                evalScripts = getEvalScriptAbsolutePath(ConfigExtension.getStormMachineResourcePanicCheckDir(conf));
            } else if (status.getType().equals(MachineCheckStatus.StatusType.error)) {
                evalScripts = getEvalScriptAbsolutePath(ConfigExtension.getStormMachineResourceErrorCheckDir(conf));
            } else if (status.getType().equals(MachineCheckStatus.StatusType.warning)) {
                evalScripts = getEvalScriptAbsolutePath(ConfigExtension.getStormMachineResourceWarningCheckDir(conf));
            }

            if (evalScripts != null && evalScripts.size() > 0) {
                for (String command : evalScripts) {
                    ScriptProcessLauncher scriptProcessLauncher = new ScriptProcessLauncher(command, timeOut, status);
                    ExitStatus exit = scriptProcessLauncher.launch();
                    if (exit.equals(ExitStatus.FAILED)) {
                        return;
                    }
                }
                status.updateInfo();
            } else {
                status.updateInfo();
                LOG.debug("jstorm machine resource " + status.getType() + "'s check scripts is non-existent");
            }
        } catch (Exception e) {
            LOG.error("Failed to run machine resource check scripts: " + e.getCause(), e);
            status.updateInfo();
        }
    }

    private static List<String> getEvalScriptAbsolutePath(String scriptDir) {
        if (scriptDir == null) {
            LOG.debug("jstorm machine resource check script directory is not configured, please check .");
            return null;
        }
        File parentFile = new File(scriptDir);
        return getChildLlist(parentFile);
    }

    private static List<String> getChildLlist(File parentFile) {
        List<String> ret = new ArrayList<String>();
        if (parentFile.exists()) {
            File[] list = parentFile.listFiles();
            if (list == null) {
            	return ret;
            }
            for (File file : list) {
                if (!file.isDirectory() && file.canExecute())
                    ret.add(file.getAbsolutePath());
            }
        }
        LOG.debug("valid scripts are {}", ret);
        return ret;
    }

    static class ScriptProcessLauncher {

        ShellUtils.ShellCommandExecutor shexec = null;

        MachineCheckStatus status = null;

        String execScript;

        public ScriptProcessLauncher(String execScript, long timeOut, MachineCheckStatus status) {
            this.status = status;
            this.execScript = execScript;
            String[] execString = {execScript};
            this.shexec = new ShellUtils.ShellCommandExecutor(execString, null, null, timeOut);
        }

        public ExitStatus launch() {
            ExitStatus exitStatus = ExitStatus.SUCCESS;
            try {
                shexec.execute();
            } catch (ShellUtils.ExitCodeException e) {
                exitStatus = ExitStatus.FAILED_WITH_EXIT_CODE;
                if (shexec.isTimedOut()) {
                    exitStatus = ExitStatus.TIMED_OUT;
                }
                LOG.warn(execScript + " exitCode exception, exit code : " + e.getExitCode() + "; the exitStatus is: " + exitStatus);
            } catch (Exception e) {
                if (!shexec.isTimedOut()) {
                    exitStatus = ExitStatus.FAILED_WITH_EXCEPTION;
                } else {
                    exitStatus = ExitStatus.TIMED_OUT;
                }
                LOG.warn(execScript + " exception, the exitStatus is: " + exitStatus);
                LOG.warn(JStormUtils.getErrorInfo(e));
            } finally {
                if (exitStatus == ExitStatus.SUCCESS && notPassed(shexec.getOutput())) {
                    exitStatus = ExitStatus.FAILED;
                    LOG.info("Script execute output: " + shexec.getOutput());
                }
            }
            return exitStatus;
        }

        private boolean notPassed(String output) {
            String[] splits = output.split("\n");
            for (String split : splits) {
                if (split.startsWith(NO_RESOURCES)) {
                    return true;
                }
            }
            return false;
        }
    }

    private enum ExitStatus {
        SUCCESS, TIMED_OUT, FAILED_WITH_EXIT_CODE, FAILED_WITH_EXCEPTION, FAILED, NO_RESOURCE
    }

    public static void main(String[] args) {
        health.check();
    }
}
