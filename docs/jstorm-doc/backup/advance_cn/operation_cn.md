---
title: 运维经验总结
layout: plain_cn
top-nav-title: 运维经验总结
top-nav-group: 进阶
top-nav-pos: 9
sub-nav-title: 运维经验总结
sub-nav-group: 进阶
sub-nav-pos: 9
---
* 启动supervisor或nimbus最好是以后台方式启动， 避免终端退出时向jstorm发送信号，导致jstorm莫名其妙的退出
```
nohup jstorm supervisor >/dev/null 2>&1 &
```

* 推荐使用admin用户启动所有的程序， 尤其是不要用root用户启动web ui， 
* 在安装目录下，建议使用jstorm-current链接， 比如当前使用版本是jstorm 0.9.4， 则创建链接指向jstorm-0.9.4， 当以后升级时， 只需要将jstorm-current链接指向新的jstorm版本。
```
ln -s jstorm-0.9.4 jstorm-current
```

* 将JStorm的本地目录和日志配置到一个公共目录下， 比如/home/admin/jstorm_data 和/home/admin/logs， 不要配置到$JSTORM_HOME/data和$JSTORM_HOME/logs，当升级时，替换整个目录时， 容易丢失所有的本地数据和日志。
* JStorm支持环境变量JSTORM_CONF_DIR, 当设置了该变量时， 会从该目录里读取storm.yaml文件， 因此建议设置该变量，该变量指定的目录存放配置文件storm.yaml， 以后每次升级时，就可以简单的替换目录就可以了
* 建议不超过1个月，强制重启一下supervisor， 因为supervisor是一个daemon进程， 不停的创建子进程，当使用时间过长时， 文件打开的句柄会非常多，导致启动worker的时间会变慢，因此，建议每隔一周，强制重启一次supervisor 
* JStorm web ui推荐使用apache tomcat 7.x， 默认的端口是8080， 如果需要将80 端口重定向到8080时， 可以用root执行命令：
```
iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080
```

* Jvm GC 需要使用CMS GC 方式， JStorm默认已经设置， 使用Storm的朋友需要类似的设置，
```
worker.childopts: "-Xms1g -Xmx1g -Xmn378m -XX:SurvivorRatio=2 -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=65"
```

* 对于一些重要的应用，可以对大集群进行分组， 修改配置文件的 “storm.zookeeper.root” 和 “nimbus.host” 
* Zeromq推荐2.1.7
  * 64位java 就需要使用64位zeromq
  * 在64位OS上使用32位java， 编译zeromq 增加flag –m32
* 对于应用使用ZK较频繁的，需要将JStorm的ZK 和应用的ZK 隔离起来，不混在一起使用
* nimbus节点上建议不运行supervisor， 并建议把nimbus放置到ZK 所在的机器上运行
* 推荐slot数为 ”CPU 核 - 1“， 假设24核CPU， 则slot为23
* 配置cronjob，定时检查nimbus和supervisor，一旦进程死去，自动重启
* ZK 的maxClientCnxns=500
* Linux对外连接端口数限制，TCP client对外发起连接数达到28000左右时，就开始大量抛异常，需要
```
 # echo "10000 65535" > /proc/sys/net/ipv4/ip_local_port_range
```