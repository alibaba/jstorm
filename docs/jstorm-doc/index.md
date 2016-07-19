---
title: "Scalable Stream Data Processing"
layout: base
---

<div class="row">
  <div class="col-sm-12"><p class="lead" markdown="span">**Alibaba JStorm** is an open source platform for scalable stream data processing.</p></div>
</div>



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