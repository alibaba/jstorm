---
title: "Enterprise Streaming Process System"
layout: base
---

<div class="row">
  <div class="col-sm-12"><p class="lead" markdown="span">**Alibaba JStorm** is an enterprise fast and stable  streaming process engine.</p></div>
</div>

<div class="row">
  <div class="col-md-6" markdown="1">

# Speed  

Run program up to 4x faster than Apache Storm. It is easy to switch from record mode to mini-batch mode.


  <div class="col-md-6 stack text-center">
    <img src="{{ site.baseurl }}/img/performance/performance1.png" alt="JStorm Performance" width="480px" height="280px">
  </div>
  </div>
  
  <div class="col-md-6" markdown="1">
# Enterprise Exactly－Once

The Enterprise Exactly-once Framework has been used to in tons of application, especially in some critical billing system such as Cainiao, Alimama, Aliexpress and so on. it has been approved as stable and correct. What's more, the performance is better than the old "At-least-once through acker"


  <div class="col-md-6 stack text-center">
    <img src="{{ site.baseurl }}/img/performance/exactly-once.png" alt="Exactly-Once" height="240px">
  </div>
  </div>
  
</div>

---

<div class="row">
  


  <div class="col-md-6" markdown="1">
# JStorm Ecosystem 

JStorm is not only a streaming process engine. It means one solution for real time requirement, whole realtime ecosystem.

  

  <div class="col-md-6 stack text-center">
    <img src="{{ site.baseurl }}/img/jstorm-ecosystem.png" alt="JStorm Ecosystem" width="480px">
  </div>
  </div>

    <div class="col-md-6" markdown="1">
# Ease of Use 

Contain the Apache Storm API,  application is easy to migrate from Apache Storm to JStorm. Most of application basing Apache Storm 0.9.5 can directly run on JStorm 2.1.1 without recompile the code.
  </div>
  
</div>
---


<div class="row">
  <div class="col-sm-6" markdown="1">
## Getting Started

Download the **latest stable release** and run JStorm on your machine, cluster, or cloud:

<div class="text-center download-button">
  <a href="Downloads.html" class="btn btn-primary" markdown="1">**Download** Alibaba JStorm {{ site.stable }}</a>
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

