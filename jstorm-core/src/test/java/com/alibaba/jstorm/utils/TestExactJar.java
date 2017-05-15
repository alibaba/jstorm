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
