package com.alibaba.jstorm.utils;

import backtype.storm.utils.Utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class LoadConf {
    private static final Logger LOG = LoggerFactory.getLogger(LoadConf.class);

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param name
     * @param mustExist   -- if this is true, the file must exist, otherwise throw exception
     * @param canMultiple -- if this is false and there is multiple conf, it will throw exception
     * @return
     */
    public static Map findAndReadYaml(String name, boolean mustExist, boolean canMultiple) {
        InputStream in = null;
        boolean confFileEmpty = false;
        try {
            in = getConfigFileInputStream(name, canMultiple);
            if (null != in) {
                Yaml yaml = new Yaml(new SafeConstructor());
                Map ret = (Map) yaml.load(new InputStreamReader(in));
                if (null != ret) {
                    return new HashMap(ret);
                } else {
                    confFileEmpty = true;
                }
            }

            if (mustExist) {
                if (confFileEmpty)
                    throw new RuntimeException("Config file " + name + " doesn't have any valid storm configs");
                else
                    throw new RuntimeException("Could not find config file on classpath " + name);
            } else {
                return new HashMap();
            }
        } catch (IOException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Invalid configuration ").append(name).append(":").append(e.getMessage());
            throw new RuntimeException(sb.toString(), e);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static InputStream getConfigFileInputStream(String configFilePath, boolean canMultiple) throws IOException {
        if (null == configFilePath) {
            throw new IOException("Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));
        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);
            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1 && canMultiple == false) {
            throw new IOException("Found multiple " + configFilePath + " resources. You're probably bundling the Storm jars with your topology jar. "
                    + resources);
        } else {
            LOG.debug("Using " + configFilePath + " from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }

    public static InputStream getConfigFileInputStream(String configFilePath) throws IOException {
        return getConfigFileInputStream(configFilePath, true);
    }

    public static Map LoadYaml(String confPath) {

        return findAndReadYaml(confPath, true, true);

    }

    public static Map LoadProperty(String prop) {

        InputStream in = null;
        Properties properties = new Properties();

        try {
            in = getConfigFileInputStream(prop);
            properties.load(in);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("No such file " + prop);
        } catch (Exception e1) {
            throw new RuntimeException("Failed to read config file");
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Map ret = new HashMap();
        ret.putAll(properties);
        return ret;
    }

    public static String getStormYamlPath() {
        String confFile = System.getProperty("storm.conf.file");
        if (!StringUtils.isBlank(confFile)) {
            return confFile;
        } else {
            return new File(System.getProperty("jstorm.home"), "conf/storm.yaml").getAbsolutePath();
        }
    }

    public static void dumpYaml(Map conf, String file) {
        // flush new conf to storm.yaml
        Yaml yaml = new Yaml();
        try {
            Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            yaml.dump(conf, writer);
        } catch (Exception ex) {
            LOG.error("Error:", ex);
        }
    }

    public static Map loadYamlFromString(String configStr) {
        if (configStr == null) {
            return null;
        }

        Yaml yaml = new Yaml();
        try {
            return yaml.loadAs(configStr, Map.class);
        } catch (Exception ex) {
            LOG.error("Error:", ex);
        }
        return null;
    }

    public static void backupAndOverwriteStormYaml(String confData) {
        String path = getStormYamlPath();
        LOG.info("backing current storm.yaml...");
        for (int i = 2; i > 0; i--) {
            mv(path + "." + i, path + "." + (i + 1));
        }
        mv(path, path + ".1");

        LOG.info("updating storm.yaml at: {}", path);
        Utils.flushToFile(path, confData, false);
    }

    public static void mv(String src, String dest) {
        try {
            FileUtils.moveFile(new File(src), new File(dest));
        } catch (Exception ignored) {
        }
    }
}
