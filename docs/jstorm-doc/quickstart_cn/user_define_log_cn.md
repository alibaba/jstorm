---
title: 自定义日志
layout: plain_cn
top-nav-title: 自定义日志
top-nav-group: 快速开始
top-nav-pos: 8
sub-nav-title: 自定义日志
sub-nav-group: 快速开始
sub-nav-pos: 8
---
本文档将告诉JStorm使用者，如何自定义日志

## 强烈建议使用Log4j
在jstorm中，底层强制使用log4j，在jstorm 0.9.0 release时， jstorm升级到logback，但遭遇大量日志兼容性问题，需要应用修改代码。因此在jstorm 0.9.1 后，回退到log4j， 其实jstorm是鼓励用户使用log4j，而不是logback， 因为log4j的兼容性远远好于logback，一个应用需要依赖大量的二方包，这些二方包使用各种日志系统和日志框架，千变万化，但有一点肯定会支持log4j，但不一定支持logback，另外虽然logback的性能要优于log4j，但有什么系统会一天打印几十G的文件呢，因此logback并不能带来应用性能的提升，却带来大量兼容性问题。
所以在使用过程中， 强烈建议使用Log4j

## 使用自定义Log4j的配置文件
在早期jstorm 版本中，发现大量应用hack的日志系统，并且使用千奇百怪的日志配置文件，遭遇了大量问题，后来从jstorm 0.9.2 后，强制log4j使用jstorm的log4j配置文件，避免了很多问题，但也带来了一些问题。用户不能自定义日志。用户有一个特殊的日志需求时，基本很难满足。
从0.9.6.3 开始，提供自定义日志功能。

使用配置项

```
user.defined.log4j.conf: xxxxxxxxxxxxxxxxxxxxx
````

或者

```
ConfigExtension.setUserDefinedLog4jConf((Map conf, String fileName))
```

举例1：

```
user.defined.log4j.conf: user.log4j.properties
```

此时配置文件user.log4j.properties必须在classpath中，因此，需要打包在用户的jar中

举例2

```
user.defined.log4j.conf: “File:/home/admin/jstorm/conf/user.log4j.properties”
```

此时配置文件就是一个明确绝对路径的文件， 每天机器上必须放置这个配置文件


## 使用logback
因为有些应用，把多个进程的日志打到一个文件中去，log4j无法完成该功能，会强制使用logback



### 只使用logback，不使用log4j
先介绍一下log4j和logback背景知识， 在java世界里， 日志框架通常有log4j 和slf4j， 而日志实现常用有log4j和logback， 
通常情况下：

```
log4j --> log4j
slf4j --> logback
```

如果应用需要log4j的代码能被logback打印，则需要log4j-over-slf4j桥接器， 同样，如果想sl4j的代码能被log4j打印，则需要sl4j-log4j桥接器

```
log4j --> log4j-over-sl4j --> logback
slf4j --> slf4j-log4j     --> log4j
```

但log4j-over-sl4j 和slf4j-log4j 是2个相互冲突的jar，不允许在同一个classloader内部。

当只使用logback时，maven需要把所有的slf4j-log4j12排除掉

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

另外在配置文件中，需要设置

```
user.defined.logback.conf: xxxxxxxxxxxxxxxx
```

或使用

```
ConfigExtension.setUserDefinedLogbackConf(Map conf, String fileName)
```

xxxxxxxxxxxx可以是一个绝对路径文件， 也可以是一个在classpath路径， 另外jstorm内部提供jstorm_home变量， 比如“%JSTORM_HOME%/conf/cluster.xml”


提交任务是，最好加上参数--exclude-jars slf4j-log4j, 这样就可以避免slf4j-log4j被加载

```
jstorm jar --exclude-jars slf4j-log4j xxxxx.jar   kkkkkkk.kkkk.kkkkk ttt
```

### 同时使用Log4j和Logback。
如果想同时使用log4j和logback， 则需要打开classloader。
1. 所有用log4j的代码，最终都会用log4j打印，无论是jstorm的打印还是应用的打印
2. 应用使用slf4j的代码，最终会用logback打印

另外注意， log4j和logback不要把日志打到同一个文件上，否则，当rolling时，会有日志丢失问题。

此种情况下，maven的pom.xml 不需要特别设置，不过，建议还是把所有的slf4j-log4j12排除掉

```
topology.enable.classloader: true
user.defined.logback.conf: xxxxxxxxxxxxxxxx
```