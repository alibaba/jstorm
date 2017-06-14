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

/**
 * @author fengjian on 16/10/26.
 */
public class JDKInfo {
    private static final String JAVA_VERSION = System.getProperty("java.version").toLowerCase();

    public static boolean isJdk8() {
        return JAVA_VERSION.split("\\.")[1].equals("8");
    }

    public static boolean isJdk7() {
        return JAVA_VERSION.split("\\.")[1].equals("7");
    }

    public static boolean lessThanJdk7() {
        return Integer.parseInt(JAVA_VERSION.split("\\.")[1]) < 7;
    }

    public static String getFullJavaVersion(){
        return JAVA_VERSION;
    }
    public static void main(String[] args) {
        System.out.println(System.getProperty("java.version"));
        System.out.println(String.valueOf(JDKInfo.isJdk8()));
        System.out.println(String.valueOf(JDKInfo.isJdk7()));
        System.out.println(getFullJavaVersion());
        System.out.println(lessThanJdk7());
    }
}
