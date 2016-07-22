---
title:  "Build JStorm Source Code"
# Top-level navigation
top-nav-group: QuickStart_cn
top-nav-pos: 6
top-nav-title: 编译JStorm
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}


# 编译jstorm

jstorm非常容易编译打包

```
mvn clean
mvn package assembly:assembly -Dmaven.test.skip=true

```

如果发现有编译错误， 非常欢迎提交pull request


# 提交patch 到jstorm社区

* fork jstorm 分支 到自己的分支下面
* 在自己分支下创建一个branch

```
$ git checkout master
$ git fetch origin
$ git merge origin/master
$ git checkout -b <local_test_branch>  # e.g. git checkout -b STORM-1234

```
* 修改代码
* 提交代码到自己的branch
* 提交pull request，  在github 自己的页面上点击"Pull Request按钮"