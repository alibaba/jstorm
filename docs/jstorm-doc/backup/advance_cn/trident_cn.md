---
title: Trident
layout: plain_cn
top-nav-title: Trident
top-nav-group: 进阶
top-nav-pos: 7
sub-nav-title: Trident
sub-nav-group: 进阶
sub-nav-pos: 7
---
Trident是Storm之上的高级抽象，提供了joins，grouping，aggregations，fuctions和filters等接口。如果你使用过Pig或Cascading，对这些接口就不会陌生。

Trident将stream中的tuples分成batches进行处理，API封装了对这些batches的处理过程，保证tuple只被处理一次。处理batches中间结果存储在TridentState对象中。

Trident事务性原理这里不详细介绍，有兴趣的读者请自行查阅资料。

参考：[https://github.com/nathanmarz/storm/wiki/Trident-tutorial](https://github.com/nathanmarz/storm/wiki/Trident-tutorial)

中文翻译可以参考： [http://blog.csdn.net/derekjiang/article/details/9126185](http://blog.csdn.net/derekjiang/article/details/9126185)
