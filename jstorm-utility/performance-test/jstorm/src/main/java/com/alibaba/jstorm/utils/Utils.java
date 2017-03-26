package com.alibaba.jstorm.utils;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.Config;


public class Utils {
    public static Map LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            return LoadConf.LoadYaml(arg);
        } else {
            return LoadConf.LoadProperty(arg);
        }
    }
    
    public static Config getConfig(String[] args) {
        Config ret = new Config();
        if (args == null || args.length == 0) {
            return ret;
        }
        
        if (StringUtils.isBlank(args[0])) {
            return ret;
        }
        
        Map conf = LoadConf(args[0]);
        ret.putAll(conf);
        return ret;
    }
}
