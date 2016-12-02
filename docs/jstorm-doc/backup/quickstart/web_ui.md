---
title: Manage Multi-cluster By Single Web UI
layout: plain
top-nav-title: Manage Multi-cluster By Single Web UI
top-nav-group: quickstart
top-nav-pos: 9
sub-nav-title: Manage Multi-cluster By Single Web UI
sub-nav-group: quickstart
sub-nav-pos: 9
---
User is able to manage multiple JStorm clusters using one single web UI after JStorm 0.9.6.1 which only needs some additional configurations. 

With the growing size of cluster and increasing number of clusters, especially small clusters, it is very difficult to launch a Web UI for each single cluster. Besides, normally launching a new Web UI for online applications requires examinations and approval in company, which will take a lot of time, so it's very necessary to manage multiple JStorm clusters using one single web UI.

##Preparation

* A running web ui.  [How to Install Web UI]({{site.baseu}}/quickstart/install.html#install-jstorm-web-ui) 
* The version of Web UI must align with the highest version of Jstorm cluster

##Add a new cluster
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
