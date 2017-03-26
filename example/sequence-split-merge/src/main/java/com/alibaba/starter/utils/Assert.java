package com.alibaba.starter.utils;

import java.io.File;
import java.io.IOException;

import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Assert, avoid include junit depenency
 * 
 * @author longda
 *        
 */
public class Assert {
    public static Logger LOG = LoggerFactory.getLogger(Assert.class);
    public static String ERROR_FILE_NAME = "error.msg";
    
    public static void dumpError(String errMsg) {
        File file = new File(ERROR_FILE_NAME);
        
        try {
            FileUtils.writeStringToFile(file, "\n!!!!!!!!!!!!!!!!!!!!!!\n", true);
            FileUtils.writeStringToFile(file, errMsg, true);
            FileUtils.writeStringToFile(file, "\n!!!!!!!!!!!!!!!!!!!!!!\n", true);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to dump " + errMsg, e);
        }
        
    }
    
    public static void assertTrue(String errMsg, boolean condition) {
        if (condition == false) {
            fail(errMsg);
        }
    }
    
    public static void fail(String errMsg) {
        RuntimeException exception = new RuntimeException(errMsg);
        dumpError(JStormUtils.getErrorInfo(exception));
        throw exception;
    }
}
