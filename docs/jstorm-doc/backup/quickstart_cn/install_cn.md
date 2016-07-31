---
title: 如何安装
layout: plain_cn
top-nav-title: 如何安装
top-nav-group: 快速开始
top-nav-pos: 3
sub-nav-title: 如何安装
sub-nav-group: 快速开始
sub-nav-pos: 3
---
# 安装步骤
* 从[下载页面]({{site.baseurl}}/downloads_cn/)下载relase包
* 搭建Zookeeper集群
1.  安装Python 2.6
2.  安装Java 
3.  配置$JSTORM_HOME/conf/storm.yaml
4.  搭建Web UI
* 启动JStorm集群

## 搭建Zookeeper集群

本处不细描叙Zookeeper安装步骤

* 安装步骤麻烦参考 [”Zookeeper 安装步骤“](http://www.cnblogs.com/linjiqin/archive/2013/03/16/2962597.html)
* Zookeeper配置麻烦参考 [“Zookeeper 配置介绍”](http://blog.csdn.net/hi_kevin/article/details/7089358)

## 搭建JStorm集群

### 安装Python 2.6
* 如果当前系统提供Python，可以不用安装Python
* 自己可以参考 [python](http://weixiaolu.iteye.com/blog/1617440)
* 也可以使用https://github.com/utahta/pythonbrew 来安装python

```  
curl -kL http://xrl.us/pythonbrewinstall | bash 
[[ -s $HOME/.pythonbrew/etc/bashrc ]] && source $HOME/.pythonbrew/etc/bashrc
pythonbrew install 2.6.7
pythonbrew switch 2.6.7
```

### 安装JDK
注意，如果当前系统是64位系统，则需要下载64位JDK，如果是32为系统，则下载32位JDK


### 安装JStorm
假设以jstorm-0.9.6.3.zip为例

```
unzip jstorm-0.9.6.3.zip
vi ~/.bashrc
export JSTORM_HOME=/XXXXX/XXXX
export PATH=$PATH:$JSTORM_HOME/bin
```

### 配置$JSTORM_HOME/conf/storm.yaml

配置项：

* storm.zookeeper.servers: 表示zookeeper 的地址，
* nimbus.host: 表示nimbus的地址
* storm.zookeeper.root: 表示JStorm在zookeeper中的根目录，**当多个JStorm共享一个zookeeper时，需要设置该选项**，默认即为“/jstorm”
* storm.local.dir: 表示JStorm临时数据存放目录，需要保证JStorm程序对该目录有写权限
* java.library.path: Zeromq 和java zeromq library的安装目录，默认"/usr/local/lib:/opt/local/lib:/usr/lib"
* supervisor.slots.ports: 表示Supervisor 提供的端口Slot列表，注意不要和其他端口发生冲突，默认是68xx，而Storm的是67xx
* topology.enable.classloader: false, 默认关闭classloader，如果应用的jar与JStorm的依赖的jar发生冲突，比如应用使用thrift9，但jstorm使用thrift7时，就需要打开classloader。建议在集群级别上默认关闭，在具体需要隔离的topology上打开这个选项。

在提交jar的节点上执行:

```
#mkdir ~/.jstorm
#cp -f $JSTORM_HOME/conf/storm.yaml ~/.jstorm
```

### 安装JStorm Web UI
必须使用tomcat 7.0 或以上版本， 注意不要忘记拷贝 **~/.jstorm/storm.yaml**

Web UI 可以和Nimbus不在同一个节点

```
mkdir ~/.jstorm
cp -f $JSTORM_HOME/conf/storm.yaml ~/.jstorm
下载tomcat 7.x （以apache-tomcat-7.0.37 为例）
tar -xzf apache-tomcat-7.0.37.tar.gz
cd apache-tomcat-7.0.37
cd webapps
cp $JSTORM_HOME/jstorm-ui-0.9.6.3.war ./
mv ROOT ROOT.old
ln -s jstorm-ui-0.9.6.3 ROOT  这个地方可能变化，是根据你的JStorm版本来确定，比如当0.9.6.1时，是ln -s jstorm-0.9.6.1 ROOT
                              另外不是 ln -s jstorm-ui-0.9.6.3.war ROOT 这个要小心
cd ../bin
./startup.sh
```

### 启动JStorm
* 在nimbus 节点上执行 “nohup jstorm nimbus &”, 查看$JSTORM_HOME/logs/nimbus.log检查有无错误
* 在supervisor节点上执行 “nohup jstorm supervisor &”, 查看$JSTORM_HOME/logs/supervisor.log检查有无错误