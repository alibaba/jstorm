---
title: "企业级流式计算引擎"
layout: base_cn
---

<div class="row">
  <div class="col-sm-12"><p class="lead" markdown="span">**Alibaba JStorm** 是一个强大的企业级流式计算引擎</p></div>
</div>

<div class="row">
  <div class="col-md-6" markdown="1">

# 性能  

是Apache Storm 的4倍性能， 可以自由切换行模式或mini-batch 模式


  <div class="col-md-6 stack text-center">
    <img src="{{ site.baseurl }}/img/performance/performance1.png" alt="JStorm Performance" width="480px" height="280px">
  </div>
  </div>
  
  <div class="col-md-6" markdown="1">
# 企业级Exactly－Once

JStorm 提供企业级Exactly-Once 编程框架， 并且JStorm的企业级Exactly-Once 编程框架已经大面积使用在阿里巴巴关键的核心应用上，如菜鸟，阿里妈妈，支付宝的计费或统计系统上。并且它的性能远超传统的Acker模式(至少一次模式).



  <div class="col-md-6 stack text-center">
    <img src="{{ site.baseurl }}/img/performance/exactly-once.png" alt="Exactly-Once" height="240px">
  </div>
  </div>
  
</div>

---

<div class="row">
  


  <div class="col-md-6" markdown="1">
# JStorm 生态

JStorm 不仅提供一个流式计算引擎， 还提供实时计算的完整解决方案， 涉及到更多的组件， 如jstorm-on-yarn, jstorm-on-docker, SQL Engine, Exactly-Once Framework 等等。
  

  <div class="col-md-6 stack text-center">
    <img src="{{ site.baseurl }}/img/jstorm-ecosystem.png" alt="JStorm Ecosystem" width="480px">
  </div>
  </div>

    <div class="col-md-6" markdown="1">
# 方便使用

JStorm 是Apache Storm的超集， 包含所有Apache Storm的API, 应用可以轻松从Apache Storm迁移到JStorm上。 并且如果是基于Apache Storm 0.9.5 的应用可以无需修改无缝迁移到JStorm 2.1.1 版本上。

  </div>
  
</div>
---


<div class="row">
  <div class="col-sm-6" markdown="1">
## Getting Started

Download the **latest stable release** and run JStorm on your machine, cluster, or cloud:

<div class="text-center download-button">
  <a href="downloads.html" class="btn btn-primary" markdown="1">**Download** Alibaba JStorm {{ site.stable }}</a>
  <a href="https://github.com/alibaba/jstorm" class="btn btn-info" markdown="1">Alibaba JStorm on **GitHub**</a>
</div>

The documentation contains a [setup guide]({{ site.baseurl }}/QuickStart_cn/Deploy) for all deployment options.

The [programming guide]({{ site.baseurl }}/QuickStart_cn/Example.html) contains all information to get you started with writing and testing your JStorm programs.


</div>

<div class="col-sm-6" markdown="1" style="padding-bottom:1em">
## Latest News

<ul class="list-group">
{% for post in site.posts limit:5 %}  
      <li class="list-group-item"><span>{{ post.date | date_to_string }}</span> &raquo;
        <a href="{{ site.baseurl }}{{ post.url }}">{{ post.title }}</a>
      </li>
{% endfor %}
</ul>

**Check out [the news](blog/) for all posts.**
  </div>
</div>

