---
title: Operation Experience
layout: plain
top-nav-title: Operation Experience
top-nav-group: advance
top-nav-pos: 9
sub-nav-title: Operation Experience
sub-nav-group: advance
sub-nav-pos: 9
---
Operation Experience

1. Supervisor or nimbus start in daemon mode , Avoid terminal exit, sending a signal to jstorm, leading to unexpectedly quit jstorm;
```
    $: nohup jstorm supervisor >/dev/null 2>&1 &
```
2. It is recommended to use the admin user starts all programs, especially not use the root user;

3. In the installation directory, it is recommended to use jstorm-current link, such as the currently used version is jstorm 0.9.6.1, then create a soft link to jstorm-0.9.6.1, when after the upgrade, simply link jstorm-current to the new jstorm version;
```
   $: ln -s jstorm-0.9.6.1 jstorm-current
```
4. Recommended setting JStorm's local data and log directory to a common directory, such as "/home/admin/jstorm_data" and "/home/admin/logs", don't configure to $JSTORM_HOME/data and $JSTORM_HOME/logs, because when you upgrade, replace the entire directory, easy to lose all local data and logs.

5. JStorm support environment variables JSTORM CONF_DIR, when this variable is set, Jstorm reads storm.yaml files from this directory, it is recommended to set this variable, the directory specified by the variable to store configuration files storm.yaml, after each upgrade , you can simply replace the directory;

6. Recommends restart the supervisor every week, because the supervisor is a daemon process, constantly fork child processes, when run for too long, the supervisor's open handle is pretty high, resulting in launching a new worker  is slow, Therefore, suggest every other week, forced to restart supervisor once;

7. At the time of kill or rebalance a topology, it is best to deactivate this topology, then wait MSG_TIMEOUT time, and then kill or rebalance;

8. JStorm web ui recommend using apache tomcat 7.x, the default port is 8080, if you need to redirect port 80 to 8080, you can execute commands with root
```
    $:sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080
```
9. JStorm has spend much effort on tuning JVM GC, this setting can also be used in Storm;
``` 
worker.childopts: "-Xms1g -Xmx1g -Xmn378m -XX:SurvivorRatio=2 -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=65"
```
10. For some important applications can be grouped large clusters, modify "storm.zookeeper.root" and "nimbus.host" profile;

11. Recommend using netty as RPC frameworker, if user prefer to zeromq, please use 2.1.7;
      * A 64-bit Java will need to use the 64 zeromq
      * In a 64-bit OS using 32-bit Java, compile zeromq flag - m32 increase

12. If applications using ZK more frequently, you need to seperate the zk of JStorm from application's, don't mix use;

13. the ZK node never run supervisor, and suggested that the nimbus placed on the machine where runs ZK;

14. Recommended slot for "CPU core -1", if 24-core CPU, the slot number is 23

15. Configuration cronjob, regularly check nimbus and supervisor, once the process of dying, automatic restart

16. Zookeeper's maxClientCnxns=500

17. In linux, when establish one network connection between client and server, the client will also bind one temporary port, if the temporary port range is from 1000, then it is likely to conflict with JStorm's port 68xx. 
```
 $:echo "10000 65535" > /proc/sys/net/ipv4/ip_local_port_range
```

