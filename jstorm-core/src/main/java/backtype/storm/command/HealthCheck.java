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
package backtype.storm.command;

import backtype.storm.utils.ShellUtils;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.HealthStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HealthCheck {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);

    private static final String NO_RESOURCES = "no_resource";

    public static HealthStatus check() {
        Map conf = Utils.readStormConfig();

        long timeout = ConfigExtension.getStormHealthTimeoutMs(conf);
        // 1. check panic dir
        String panicPath = ConfigExtension.getStormMachineResourcePanicCheckDir(conf);
        if (!isHealthyUnderPath(panicPath, timeout)) {
            return HealthStatus.PANIC;
        }

        // 2. check error dir
        String errorPath = ConfigExtension.getStormMachineResourceErrorCheckDir(conf);
        if (!isHealthyUnderPath(errorPath, timeout)) {
            return HealthStatus.ERROR;
        }

        // 3. check warn dir
        String warnPath = ConfigExtension.getStormMachineResourceWarningCheckDir(conf);
        if (!isHealthyUnderPath(warnPath, timeout)) {
            return HealthStatus.WARN;
        }

        return HealthStatus.INFO;
    }

    /**
     * return false if the health check failed when running the scripts under the path
     **/
    private static boolean isHealthyUnderPath(String path, long timeout) {
        if (path == null) {
            return true;
        }
        List<String> commands = generateCommands(path);
        if (commands != null && commands.size() > 0) {
            for (String command : commands) {
                ScriptProcessLauncher scriptProcessLauncher = new ScriptProcessLauncher(command, timeout);
                ExitStatus exit = scriptProcessLauncher.launch();
                if (exit.equals(ExitStatus.FAILED)) {
                    return false;
                }
            }
            return true;
        } else {
            return true;
        }
    }

    private static List<String> generateCommands(String filePath) {
        File path = new File(filePath);
        List<String> commands = new ArrayList<>();
        if (path.exists()) {
            File[] files = path.listFiles();
            if (files == null) {
                return commands;
            }
            for (File file : files) {
                if (!file.isDirectory() && file.canExecute()) {
                    commands.add(file.getAbsolutePath());
                }
            }
        }
        LOG.debug("The generated check commands are {}", commands);
        return commands;
    }

    static class ScriptProcessLauncher {
        ShellUtils.ShellCommandExecutor executor = null;

        String command;

        public ScriptProcessLauncher(String command, long timeOut) {
            this.command = command;
            this.executor = new ShellUtils.ShellCommandExecutor(new String[]{command}, null, null, timeOut);
        }

        public ExitStatus launch() {
            ExitStatus exitStatus = ExitStatus.SUCCESS;
            try {
                executor.execute();
            } catch (Exception e) {
                if (executor.isTimedOut()) {
                    exitStatus = ExitStatus.TIMED_OUT;
                } else {
                    exitStatus = ExitStatus.EXCEPTION;
                }
                LOG.warn(command + " exception, the exit status is: " + exitStatus, e);
            } finally {
                if (exitStatus == ExitStatus.SUCCESS && hasNoResource(executor.getOutput())) {
                    exitStatus = ExitStatus.FAILED;
                    LOG.info("Script execute output: " + executor.getOutput());
                }
            }
            return exitStatus;
        }

        private boolean hasNoResource(String output) {
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
        SUCCESS, TIMED_OUT, EXCEPTION, FAILED
    }
}
