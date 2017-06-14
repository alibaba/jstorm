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
package com.alibaba.jstorm.config;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * yarn config black list, note that ONLY plain K-V is supported, list/map values are not supported!!!
 * so if you intend to set list values, put the values on a single line like ['xx', 'xx'].
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 16/5/26
 */
public class YarnConfigBlacklist implements Refreshable {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private static YarnConfigBlacklist INSTANCE;

    private final Splitter NEW_LINE = Splitter.on(Pattern.compile("[\n]"));
    private Map conf;
    private boolean isJstormOnYarn;
    private Set<String> yarnConfigBlackList = new HashSet<>();

    private YarnConfigBlacklist(Map conf) {
        this.conf = conf;
        this.isJstormOnYarn = JStormUtils.parseBoolean(System.getProperty("jstorm-on-yarn"), false) ||
                ConfigExtension.isJStormOnYarn(conf);
        parseYarnConfigBlackList(this.conf);
        if (isJstormOnYarn) {
            LOG.info("running jstorm on YARN.");
        } else {
            LOG.info("running jstorm in standalone mode.");
        }
    }

    public static YarnConfigBlacklist getInstance(Map conf) {
        if (INSTANCE == null) {
            INSTANCE = new YarnConfigBlacklist(conf);
            RefreshableComponents.registerRefreshable(INSTANCE);
        }
        return INSTANCE;
    }

    public boolean isJstormOnYarn() {
        return isJstormOnYarn;
    }

    @Override
    public void refresh(Map conf) {
        parseYarnConfigBlackList(conf);
        LOG.info("config blacklist:{}", Joiner.on(",").join(yarnConfigBlackList));
    }

    private void parseYarnConfigBlackList(Map conf) {
        String yarnBlackList = (String) conf.get("jstorm.yarn.conf.blacklist");
        if (!StringUtils.isBlank(yarnBlackList)) {
            yarnConfigBlackList.clear();
            String[] keys = yarnBlackList.split(",");
            for (String key : keys) {
                key = key.trim();
                if (!StringUtils.isBlank(key)) {
                    yarnConfigBlackList.add(key);
                }
            }
        }
    }

    public String filterConfigIfNecessary(String confData) {
        if (confData == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder(4096);
        Iterable<String> lines = splitLines(confData);
        List<String> lineArray = Lists.newArrayList(lines);
        for (int i = 0; i < lineArray.size(); i++) {
            String trimmedLine = lineArray.get(i).trim();
            if (!trimmedLine.startsWith("#") && trimmedLine.contains(":")) {
                String[] parts = trimmedLine.split(":");
                if (parts.length >= 2) {
                    String key = parts[0].trim();
                    if (yarnConfigBlackList.contains(key)) {
                        continue;
                    }
                } else if (parts.length == 1) {
                    String key = parts[0].trim();
                    if (yarnConfigBlackList.contains(key)) {
                        while (i + 1 < lineArray.size()) {
                            trimmedLine = lineArray.get(i + 1).trim();
                            if (!trimmedLine.startsWith("#") && !trimmedLine.contains(":")) {
                                i++;
                            } else {
                                break;
                            }
                        }
                        continue;
                    }
                }
            }
            sb.append(lineArray.get(i)).append("\n");
        }

        return sb.toString();
    }

    public String getRetainedConfig(String confData) {
        if (confData == null || !isJstormOnYarn) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        Iterable<String> lines = splitLines(confData);
        List<String> lineArray = Lists.newArrayList(lines);
        for (int i = 0; i < lineArray.size(); i++) {
            String trimmedLine = lineArray.get(i).trim();
            if (!trimmedLine.startsWith("#") && trimmedLine.contains(":")) {
                String[] parts = trimmedLine.split(":");
                if (parts.length >= 2) {
                    String key = parts[0].trim();
                    if (yarnConfigBlackList.contains(key)) {
                        sb.append(lineArray.get(i)).append("\n");
                    }
                } else if (parts.length == 1) {
                    String key = parts[0].trim();
                    if (yarnConfigBlackList.contains(key)) {
                        sb.append(lineArray.get(i)).append("\n");
                        while (i + 1 < lineArray.size()) {
                            trimmedLine = lineArray.get(i + 1).trim();
                            if (!trimmedLine.startsWith("#") && !trimmedLine.contains(":")) {
                                i++;
                                sb.append(lineArray.get(i)).append("\n");
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        }

        return sb.toString();
    }

    private Iterable<String> splitLines(String confData) {
        confData = confData.replace("\r", "");
        return NEW_LINE.split(confData);
    }
}
