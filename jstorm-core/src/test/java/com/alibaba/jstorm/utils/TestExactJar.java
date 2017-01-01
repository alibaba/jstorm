package com.alibaba.jstorm.utils;

import java.io.File;

import org.junit.Test;

import com.alibaba.jstorm.cluster.StormConfig;

import junit.framework.Assert;

public class TestExactJar {
    
    public void test(String jarPath, String tmpRoot) {
        File jarFile = new File(jarPath);
        if (jarFile.exists() == false) {
            System.out.println(jarPath + " doesn't exit");
            return ;
        }
        
        File tmpRootFile = new File(tmpRoot);
        if (tmpRootFile.exists() == false || tmpRootFile.isDirectory() == false) {
            System.out.println(jarPath + " doesn't exit");
            return ;
        }
        
        JStormUtils.extractDirFromJar(jarPath, StormConfig.RESOURCES_SUBDIR, tmpRoot);
        
        String resourcePath = tmpRoot + File.separator + StormConfig.RESOURCES_SUBDIR;
        File resourceFile = new File(resourcePath);
        Assert.assertTrue(resourcePath + " should exist", resourceFile.exists());
        String[] subFiles = resourceFile.list();
        Assert.assertTrue(resourcePath + " should exist sub files.", subFiles != null && subFiles.length > 0);
    }
    
    public static void main(String[] args) {
        String jarPath = args[0];
        String tmpRoot = "target";
        
        TestExactJar instance = new TestExactJar();
        instance.test(jarPath, tmpRoot);
    }
}
