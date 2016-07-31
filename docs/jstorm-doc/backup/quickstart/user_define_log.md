---
title: User Define log
layout: plain
top-nav-title: User Define log
top-nav-group: quickstart
top-nav-pos: 8
sub-nav-title: User Define log
sub-nav-group: quickstart
sub-nav-pos: 8
---
## Highly recommend using Log4j
In release of JStorm 0.9.0, the log system was upgraded to logback from log4j. However the upgrade triggered the compatibility problem and the application has to change the code to avoid that, so in JStorm 0.9.1 it fall back to log4j. Although logback's performance is better than log4j, logback will bring lots of compatibility issues.

## User defined Log4j configuration file
We found that in the early version of JStorm, lots of applications hack the log system and use various kinds of log configurations, which eventually caused many problems, so after JStorm 0.9.2 we force log4j to use JStorm's log4j configuration file to avoid the problems. However, user are still not able to customize the log system to satisfy the particular requirements.
In JStorm 0.9.6.3, we introduced the feature of user defined log. 

Using configuration
```
user.defined.log4j.conf: xxxxxxxxxxxxxxxxxxxxx
````
or
```
ConfigExtension.setUserDefinedLog4jConf((Map conf, String fileName))
```

For example:
```
user.defined.log4j.conf: user.log4j.properties
```
The configuration file user.log4j.properties must be on the java classpath. 

Example 2:
```
user.defined.log4j.conf: "File:/home/admin/jstorm/conf/user.log4j.properties"
```
The configuration file is an absolute file path so each machine should have the same file at the same location. 

## Using logback
Some application will output different processes's log into one log file, here user have to use logback because log4j does not support that.

### Using logback without log4j
Generally there are only two frequently used log framework in Java's world, log4j and slf4j. For log implementation, we usually have log4j and logback.
```
log4j -- > log4j
slf4j --> logback
```
If application's log4j code need to be logged by logback, then log4j-over-slf4j adapter is needed. Also, if application's sl4j code need to be logged by log4j, then slf4j-log4j adapter is needed.
```
log4j --> log4j-over-sl4j --> logback
slf4j --> slf4j-log4j      --> log4j
```
Unfortunately, log4j-over-sl4j conflicts with slf4j-log4j,they are not supposed to be loaded by the same classloader.

When only using logback,Maven have to exclude slf4j-log4j12
```
    <dependency>
    		<groupId>com.alibaba.jstorm</groupId>
			<artifactId>jstorm-client-extension</artifactId>
			<version>${jstorm.version}</version>
			<scope>provided</scope>
		</dependency>

		<dependency>
			<groupId>com.alibaba.jstorm</groupId>
			<artifactId>jstorm-client</artifactId>
			<version>${jstorm.version}</version>
			<scope>provided</scope>
			<exclusions>
				<exclusion>
					<groupId>org.slf4j</groupId>
					<artifactId>slf4j-log4j12</artifactId>
				</exclusion>
			</exclusions>
		</dependency>

		<dependency>
			<groupId>com.alibaba.jstorm</groupId>
			<artifactId>jstorm-server</artifactId>
			<version>${jstorm.version}</version>
			<scope>provided</scope>

		</dependency>

		<dependency>
			<groupId>ch.qos.logback</groupId>
			<artifactId>logback-classic</artifactId>
			<version>1.0.13</version>
		</dependency>

		<dependency>
			<groupId>org.slf4j</groupId>
			<artifactId>log4j-over-slf4j</artifactId>
			<version>1.7.10</version>
		</dependency>
```

Besides, in the configuration file
```
user.defined.logback.conf: xxxxxxxxxxxxxxxx
```
or
```
ConfigExtension.setUserDefinedLogbackConf(Map conf, String fileName)
```
xxxxxxxxxxxx could be an absolute file path or a path contained in the Java classpath.
JStorm also supports paramater ```jstorm_home```, like "%JSTORM_HOME%/conf/cluster.xml"

When submitting the topology, adding option ```--exclude-jars slf4j-log4j``` can avoid the loading of slf4j-log4j.
```
jstorm jar --exclude-jars slf4j-log4j xxxxx.jar kkkkkkk.kkkk.kkkkk ttt
```

### Using Log4j and Logback at the same time
If user want to use log4j and logback at the same time,the topology classloader should be enabled.
1. All the log4j code will be logged by log4j no matter it comes from JStorm or the application
2. The slf4j code used in the application will be logged by logback

Please note that log4j and logback should not output their log into the same file otherwise some log will loss when rolling happens.

In this situation, Maven pom.xml file does not need some special configuration but we still recommend excluding the slf4j-log4j12

```
topology.enable.classloader: true
user.defined.logback.conf: xxxxxxxxxxxxxxxx
```