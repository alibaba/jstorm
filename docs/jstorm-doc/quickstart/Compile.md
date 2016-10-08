---
title:  "Build JStorm Source Code"
# Top-level navigation
top-nav-group: QuickStart
top-nav-pos: 6
top-nav-title: Compile JStorm
---

* This will be replaced by the TOC
{:toc}

#TO BE DONE 
#need "PATCH TO THE COMMUNITY"



Build JStorm

```
git clone https://github.com/alibaba/jstorm.git
mvn clean package install
```

Build install tar 

```
mvn package assembly:assembly
```
