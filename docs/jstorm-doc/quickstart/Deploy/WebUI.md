---
title: JStorm WebUI Deploy
is_beta: false

sub-nav-group: Deploy
sub-nav-id: WebUI
sub-nav-pos: 3
---

* This will be replaced by the TOC
{:toc}

## Install JStorm Web UI
Note: Web UI can run on the node different from the Nimbus node (on any machine that can connect the cluster nodes)

Download tomcat 7.x (take apache-tomcat-7.0.37 as an example)

```
mkdir ~/.jstorm 
cp -f $JSTORM_HOME/conf/storm.yaml ~/.jstorm
tar -xzf apache-tomcat-7.0.37.tar.gz
cd apache-tomcat-7.0.37
cd webapps
cp $JSTORM_HOME/jstorm-ui-2.1.1.war ./
mv ROOT ROOT.old
ln -s jstorm-ui-2.1.1 ROOT
cd ../bin
./startup.sh
```

## Add a new cluster
Edit ~/.jstorm/storm.yaml on the machine where Web UI running, here is the example:

```
# UI MultiCluster
# Following is an example of multicluster UI configuration
 ui.clusters:
     - {
         name: "jstorm.share",
         zkRoot: "/jstorm",
         zkServers:
             [ "zk.test1.com", "zk.test2.com", "zk.test3.com"],
         zkPort: 2181,
       }
     - {
         name: "jstorm.bu1",
         zkRoot: "/jstorm.dw",
         zkServers:
             [ "10.125.100.101", "10.125.100.101", "10.125.100.101"],
         zkPort: 2181,
       }
```
Please note 
```
- {
         name: "jstorm.bu1",     --- The name of cluster must be unique
         zkRoot: "/jstorm.dw",   --- The root ZK node of the cluster, please refer "storm.zookeeper.root" in $JSTORM_HOME/con/storm.yaml
         zkServers:
             [ "10.125.100.101", "10.125.100.101", "10.125.100.101"],  
         zkPort: 2181,                                                  
       }
```

