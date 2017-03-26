---
title:  "BlobStore 使用指南"
layout: plain_cn


# Top-level navigation
sub-nav-group: AdvancedUsage_cn
sub-nav-id: BlobStore_cn
#sub-nav-pos: 7
sub-nav-title: BlobStore
---

* This will be replaced by the TOC
{:toc}

# BlobStore 使用指南

## Nimbus Local 模式

Blobstore 可以是用nimbus本地存储，但是需要 zookeeper 的介入来保证一致性。要使用nimbus local 模式的blobstore，默认的集群配置就可以了。默认的关键的几个配置如下：

```
## supervisor 与 blobstore 交互使用的客户端，对于本地模式，使用 NimbusBlobStore
supervisor.blobstore.class: "com.alibaba.jstorm.blobstore.NimbusBlobStore"
## nimbus上用来实现blobstore的类，对于本地模式，使用LocalFsBlobStore
nimbus.blobstore.class: "com.alibaba.jstorm.blobstore.LocalFsBlobStore"
```

本地模式blobstore会存放到 `storm.local.dir/blobs/` 目录下，如果想要自定义存放目录可以通过`blobstore.dir`强制指定一个绝对路径。

## HDFS 模式

Blobstore 可以使用HDFS模式，这样Nimbus就是无状态的了。HDFS模式会使用HDFS自带的备份、一致性保证，所以不需要zookeeper的介入。配置如下，nimbus和supervisor都需要加入如下配置并重启。

```
## 在HDFS上用来存放blobstore的目录
blobstore.dir: "/jstorm"
## HDFS namenode hostname/ip
blobstore.hdfs.hostname: "10.101.174.110"
## HDFS namenode port
blobstore.hdfs.port: "9000"
## supervisor 与 blobstore 交互使用的客户端，对于HDFS模式，使用HdfsClientBlobStore
supervisor.blobstore.class: "com.alibaba.jstorm.hdfs.blobstore.HdfsClientBlobStore"
## nimbus上用来实现blobstore的类，对于HDFS模式，使用HdfsBlobStore
nimbus.blobstore.class: "com.alibaba.jstorm.hdfs.blobstore.HdfsBlobStore"
## tell nimbus and supervisor to load which external plugins
nimbus.external: "hbase,hdfs"
supervisor.external: "hdfs"
```

## 旧版本迁移

如果要从旧版本（不带blobstore功能的）迁移到带有blobstore功能的版本，需要将拓扑文件进行迁移。方法非常简单，

1. 升级集群到最新版本。并只启动一个nimbus
2. 在nimbus master上进入`%JSTORM_HOME%/bin/` 运行 `jstorm blobstore -m`，将 `jstorm-local/nimbus/stormdist`的文件迁移到blobstore。

