package com.alibaba.jstorm.ui.utils;

import org.apache.commons.exec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command Created with jstorm-ui.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 16/3/1 上午11:48
 * @desc
 */
public class Command {

    private static Logger LOG = LoggerFactory.getLogger("CommandLogger");

    public static class Builder {

        public static CommandLine activate(String topologyName) {
            return CommandLine.parse("jstorm activate " + topologyName);
        }

        public static CommandLine deactivate(String topologyName) {
            return CommandLine.parse("jstorm deactivate " + topologyName);
        }

        public static CommandLine kill(String topologyName, int waitTimeSecs) {
            return CommandLine.parse("jstorm kill " + topologyName + " " + waitTimeSecs);
        }

        public static CommandLine rebalance(String topologyName, int waitTimeSecs) {
            return CommandLine.parse("jstorm rebalance " + topologyName + " " + waitTimeSecs);
        }

        public static CommandLine restart(String topologyName, String confFilePath) {
            if ( null == confFilePath ) {
                confFilePath = "";
            }
            return CommandLine.parse("jstorm restart " + topologyName + " " + confFilePath);
        }

        /** jstorm jar topology-jar-path class ... */
        public static CommandLine jar(String topologyJarPath, String clazz) {
            return CommandLine.parse("jstorm jar " + topologyJarPath + " " + clazz);
        }
    }

    public static class Processor {

        public static String execute(CommandLine commandLine) {
            final StringBuilder result = new StringBuilder();
            try {
                LOG.warn("execute " + commandLine.toString() + " start.");
                DefaultExecutor executor = new DefaultExecutor();
                ExecuteWatchdog watchdog = new ExecuteWatchdog(3 * 60 * 1000);

                //                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
                LogOutputStream outputStream = new LogOutputStream() {

                    @Override
                    protected void processLine(String line, int level) {
                        LOG.warn(line);
                        result.append(line+"\\n");
                    }
                };

                executor.setExitValue(1);
                executor.setWatchdog(watchdog);
                executor.setStreamHandler(new PumpStreamHandler(outputStream));

                executor.execute(commandLine, resultHandler);
                resultHandler.waitFor();

            } catch ( Exception e ) {
                LOG.error("execute " + commandLine.toString() + " error ", e);
                result.append(e.getMessage());
            }
            LOG.warn("execute " + commandLine.toString() + " finished.");
            return result.toString();
        }
    }

}
