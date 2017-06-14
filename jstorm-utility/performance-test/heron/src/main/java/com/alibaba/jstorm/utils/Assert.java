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
import java.io.IOException;

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
        dumpError(Utils.getErrorInfo(exception));
        throw exception;
    }
}
