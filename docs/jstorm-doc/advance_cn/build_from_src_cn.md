---
title: 源码编译
layout: plain_cn
top-nav-title: 源码编译
top-nav-group: 进阶
top-nav-pos: 13
sub-nav-title: 源码编译
sub-nav-group: 进阶
sub-nav-pos: 13
---

Build JStorm

```
git clone https://github.com/alibaba/jstorm.git
mvn clean package install
```

Build install tar 

```
mvn package assembly:assembly
```