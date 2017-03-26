---
title: Resource Grouping
layout: plain
top-nav-title: Resource Grouping
top-nav-group: advance
top-nav-pos: 4
sub-nav-title: Resource Grouping
sub-nav-group: advance
sub-nav-pos: 4
---
JStorm provide Resource Grouping in one single JStorm cluster, but this feature has been discard from 0.9.5, because in hadoop-yarn, Resource Grouping is easily to implement in high level, in this framework, there are lots of JStorm cluster running on hadoop-yarn, each logic cluster is for one BU or group, small  JStorm cluster is easily being upgraded and maintain.