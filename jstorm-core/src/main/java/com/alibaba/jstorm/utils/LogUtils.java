/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.utils;

import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.client.ConfigExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Contains methods to access and manipulate logback/log4j framework dynamically at run-time.
 * Here 'dynamically' means without referencing the logback/log4j JAR, but using it if found in the classpath.
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class LogUtils {
    private static final Logger LOG = LoggerFactory.getLogger(LogUtils.class);

    public static final String LOGBACK_CLASSIC = "ch.qos.logback.classic";
    public static final String LOGBACK_CLASSIC_LOGGER = "ch.qos.logback.classic.Logger";
    public static final String LOGBACK_CLASSIC_LEVEL = "ch.qos.logback.classic.Level";

    public static final String LOG4J_CLASSIC = "org.apache.log4j";
    public static final String LOG4J_CLASSIC_LOGGER = "org.apache.log4j.Logger";
    public static final String LOG4J_CLASSIC_LEVEL = "org.apache.log4j.Level";

    // this timestamp is used to check whether we need to change log level
    public static Long lastChangeTS = 0L;

    private LogUtils() {
        // Prevent instance creation
    }

    public static void update(Map conf) {
        Map<String, String> logLevelConfig;
        try {
            logLevelConfig = ConfigExtension.getChangeLogLevelConfig(conf);
        } catch (ClassCastException e) {
            LOG.error("the log level config is not the type of Map<String, String> !");
            return;
        }
        Long confTs = ConfigExtension.getChangeLogLevelTimeStamp(conf);
        if (logLevelConfig != null && confTs != null && confTs > lastChangeTS) {
            updateLogLevel(logLevelConfig);
            if (WorkerClassLoader.isEnable()){
                WorkerClassLoader.switchThreadContext();
                updateLogLevel(logLevelConfig);
                WorkerClassLoader.restoreThreadContext();
            }
            lastChangeTS = confTs;
        }
    }

    private static void updateLogLevel(Map<String, String> logLevelConfig){
        for (Map.Entry<String, String> entry : logLevelConfig.entrySet()) {
            String loggerName = entry.getKey();
            String level = entry.getValue();
            boolean logback = setLogBackLevel(loggerName, level);
            boolean log4j = setLog4jLevel(loggerName, level);
            if (!logback && !log4j){
                LOG.warn("Couldn't set logback level to {} for the logger '{}'", level, loggerName);
            }
        }
    }


    /**
     * Dynamically sets the logback log level for the given class to the specified level.
     *
     * @param loggerName Name of the logger to set its log level. If blank, root logger will be used.
     * @param logLevel   One of the supported log levels: TRACE, DEBUG, INFO, WARN, ERROR, FATAL,
     *                      OFF. {@code null} value is considered as 'OFF'.
     */
    public static boolean setLogBackLevel(String loggerName, String logLevel) {
        String logLevelUpper = (logLevel == null) ? "OFF" : logLevel.toUpperCase();
        try {
            Package logbackPackage = Package.getPackage(LOGBACK_CLASSIC);
            if (logbackPackage == null) {
                LOG.warn("Logback is not in the classpath!");
                return false;
            }

            // Use ROOT logger if given logger name is blank.
            if ((loggerName == null) || loggerName.trim().isEmpty()) {
                loggerName = (String) getFieldVaulue(LOGBACK_CLASSIC_LOGGER, "ROOT_LOGGER_NAME");
            }

            // Obtain logger by the name
            Logger loggerObtained = LoggerFactory.getLogger(loggerName);
            if (loggerObtained == null) {
                // I don't know if this case occurs
                LOG.warn("No logger for the name: {}", loggerName);
                return false;
            }

            Object logLevelObj = getFieldVaulue(LOGBACK_CLASSIC_LEVEL, logLevelUpper);
            if (logLevelObj == null) {
                LOG.warn("No such log level: {}", logLevelUpper);
                return false;
            }

            Class<?>[] paramTypes = {logLevelObj.getClass()};
            Object[] params = {logLevelObj};

            Class<?> clz = Class.forName(LOGBACK_CLASSIC_LOGGER);
            Method method = clz.getMethod("setLevel", paramTypes);
            method.invoke(loggerObtained, params);

            LOG.info("LogBack level set to {} for the logger '{}'", logLevelUpper, loggerName);
            return true;
        } catch (Exception e) {
//            LOG.warn("Couldn't set logback level to {} for the logger '{}'", logLevelUpper, loggerName, e);
            return false;
        }
    }

    /**
     * Dynamically sets the log4j log level for the given class to the specified level.
     *
     * @param loggerName Name of the logger to set its log level. If blank, root logger will be used.
     * @param logLevel   One of the supported log levels: TRACE, DEBUG, INFO, WARN, ERROR, FATAL,
     *                      OFF. {@code null} value is considered as 'OFF'.
     */
    public static boolean setLog4jLevel(String loggerName, String logLevel) {
        String logLevelUpper = (logLevel == null) ? "OFF" : logLevel.toUpperCase();
        try {
            Package log4jPackage = Package.getPackage(LOG4J_CLASSIC);
            if (log4jPackage == null) {
                LOG.warn("Log4j is not in the classpath!");
                return false;
            }

            Class<?> clz = Class.forName(LOG4J_CLASSIC_LOGGER);
            // Obtain logger by the name
            Object loggerObtained;
            if ((loggerName == null) || loggerName.trim().isEmpty()) {
                // Use ROOT logger if given logger name is blank.
                Method method = clz.getMethod("getRootLogger");
                loggerObtained = method.invoke(null);
                loggerName = "ROOT";
            } else {
                Method method = clz.getMethod("getLogger", String.class);
                loggerObtained = method.invoke(null, loggerName);
            }


            if (loggerObtained == null) {
                // I don't know if this case occurs
                LOG.warn("No logger for the name: {}", loggerName);
                return false;
            }

            Object logLevelObj = getFieldVaulue(LOG4J_CLASSIC_LEVEL, logLevelUpper);
            if (logLevelObj == null) {
                LOG.warn("No such log level: {}", logLevelUpper);
                return false;
            }

            Class<?>[] paramTypes = {logLevelObj.getClass()};
            Object[] params = {logLevelObj};

            Method method = clz.getMethod("setLevel", paramTypes);
            method.invoke(loggerObtained, params);

            LOG.info("Log4j level set to {} for the logger '{}'", logLevelUpper, loggerName);
            return true;
        } catch (Exception e) {
//            LOG.warn("Couldn't set log4j level to {} for the logger '{}'", logLevelUpper, loggerName);
            return false;
        }
    }


    private static Object getFieldVaulue(String fullClassName, String fieldName) {
        try {
            Class<?> clazz = Class.forName(fullClassName);
            Field field = clazz.getField(fieldName);
            return field.get(null);
        } catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | NoSuchFieldException |
                SecurityException ignored) {
            return null;
        }
    }
}
