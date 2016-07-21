---
title: "Scalable Stream Data Processing"
layout: base_cn
---

<div class="row">
  <div class="col-sm-12"><p class="lead" markdown="span">**Alibaba JStorm** 是一个非常牛X的流计算框架</p></div>
</div>

<div class="row">
  <div class="col-md-6" markdown="1">

**Flink’s core** is a [streaming dataflow engine](features.html) that provides data distribution, communication, and fault tolerance for distributed computations over data streams.

Flink includes **several APIs** for creating applications that use the Flink engine:

1. [DataStream API]({{ site.docs-snapshot }}/apis/streaming/index.html) for unbounded streams embedded in Java and Scala, and
2. [DataSet API]({{ site.docs-snapshot }}/apis/batch/index.html) for static data embedded in Java, Scala, and Python,
3. [Table API]({{ site.docs-snapshot }}/apis/batch/libs/table.html) with a SQL-like expression language embedded in Java and Scala.

Flink also bundles **libraries for domain-specific use cases**:

1. [CEP]({{ site.docs-snapshot }}/apis/streaming/libs/cep.html), a complex event processing library,
2. [Machine Learning library]({{ site.docs-snapshot }}/apis/batch/libs/ml/index.html), and
3. [Gelly]({{ site.docs-snapshot }}/apis/batch/libs/gelly.html), a graph processing API and library.

You can **integrate** Flink easily with other well-known open source systems both for [data input and output](features.html#deployment-and-integration) as well as [deployment](features.html#deployment-and-integration).
  </div>
  <div class="col-md-6 stack text-center">
    <!-- https://docs.google.com/drawings/d/1XCNHsBDAq0fP-TSazE4CcrUinrC37JFiuXAoAEZZavE/ -->
    <img src="{{ site.baseurl }}/img/flink-stack-frontpage.png" alt="Apache Flink Stack" width="480px" height="280px">
  </div>
  </div>
---

<div class="row">
  <div class="col-sm-6" markdown="1">
## Getting Started

Download the **latest stable release** and run Flink on your machine, cluster, or cloud:

<div class="text-center download-button">
  <a href="downloads.html" class="btn btn-primary" markdown="1">**Download** Apache Flink {{ site.stable }}</a>
  <a href="{{ site.github }}" class="btn btn-info" markdown="1">Apache Flink on **GitHub**</a>
</div>

The documentation contains a [setup guide]({{ site.docs-snapshot }}/setup) for all deployment options.

The [programming guide]({{ site.docs-snapshot }}/apis/programming_guide.html) contains all information to get you started with writing and testing your Flink programs.

**Check out the [documentation]({{ site.docs-snapshot }}) for the next steps.**

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

