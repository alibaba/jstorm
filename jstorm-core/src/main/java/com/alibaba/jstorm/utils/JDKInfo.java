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
