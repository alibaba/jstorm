---
title:  "Deploy"
# Top-level navigation
top-nav-group: QuickStart_cn
top-nav-pos: 3
top-nav-title: 安装部署

# Sub-level navigation
sub-nav-group: Deploy_cn
sub-nav-group-title: Deploy_cn
sub-nav-pos: 1
sub-nav-title: 安装部署
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}

# 概述
安装JStorm 分为2个步骤，

* 安装JStorm 引擎
  * Standalone,  jstorm 单独部署，不依赖外部系统，比如yarn或docker swarm
  * Yarn, 将jstorm 运行在yarn上， 会在yarn提交一个app， 这个app就是一个jstorm 逻辑集群
  * Docker, 在集群中创建一批docker， 这批docker 组成一个jstorm 的逻辑集群。
* 安装JStorm UI

# Standalone 部署方式

最简单， 最轻量， 最稳定，也是最常用的方式。当整体规模不超过300台时，Standalone是最简单的方式。

# Yarn 部署方式
当规模超过300台时，可以考虑使用yarn的方式

## 优点：
 * 天然支持多租户， 在一个物理集群上，创建很多jstorm逻辑集群， 每一个jstorm 逻辑集群就为一个bu 或一个team服务，并且逻辑集群和逻辑集群之间，天然支持资源隔离
 * 提高资源利用率， 大量的小的standalone 集群， 必然出现，有的集群利用率低，有的集群利用率高，并且利用率低的集群资源无法共享给利用率高的集群， 大家集中在一起，必然可以削峰填谷，提供更多的资源。
 * 会给运维带来很大的方便， 创建新集群，升级，扩容， 缩容 都极为方便。
 * 可以和spark－streaming， flink， 混用

## 缺点：
 * 稳定性比standalone 会有下降， 如果yarn集群还和离线应用混在一起， 很容易离线应用将网卡打满，抢占了在线应用的资源。因此建议yarn上应用都是在线或实时应用。
 * 外部依赖比较重， 依赖一个重量级的yarn集群， 规模至少在30台以上才有必要玩yarn 模式

# Docker:
 当资源池比较林散时，或公司有成熟的docker 运维平台时，可以考虑这种方式

## 优点：
 * 天然支持多租户， 在一个物理集群上，创建很多jstorm逻辑集群， 每一个jstorm 逻辑集群就为一个bu 或一个team服务，并且逻辑集群和逻辑集群之间，天然支持资源隔离
 * 提高资源利用率， 大量的小的standalone 集群， 必然出现，有的集群利用率低，有的集群利用率高，并且利用率低的集群资源无法共享给利用率高的集群， 大家集中在一起，必然可以削峰填谷，提供更多的资源。
 * 会给运维带来很大的方便， 创建新集群，升级，扩容， 缩容 都极为方便。

## 缺点：
 * 稳定性比standalone 会有下降， 因为standalone是完全独占机器或资源， 而docker 很有可能会出现，在凌晨时，将多个docker挤在一台机器上，白天时，再部署到多台机器上，会有一个颠簸时间。
 * 外部依赖比较重， 需要外部一个docker 运维管控平台才能完成
 * 相对直接在物理机上运行standalone方式，性能有一点点的损耗，这个是因为docker增加了一层虚拟化，不过，只有在极限测试的情况下，才能看得出来，真实情况下，一般性能瓶颈在应用方， 是感受不出来， 因此，使用方无需担心这个问题。 
