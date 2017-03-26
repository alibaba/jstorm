---
title: Transaction
layout: plain
top-nav-title: Transaction
top-nav-group: advance
top-nav-pos: 6
sub-nav-title: Transaction
sub-nav-group: advance
sub-nav-pos: 6
---
The transaction in JStorm is often used for high precision scenario, such as counting the amount or quantity, or synchronizing the database.

The implementation of the transaction management in storm is not that friendly for users, even becoming more abstruse after wrapping within Trident. Please refer to [Storm Transaction](http://storm.apache.org/documentation/Transactional-topologies.html) for Storm transaction details.

JStorm provide one new transaction framework, which core design is same as Storm's, but more friendly to user, please refer to [JStorm Transaction Example](https://github.com/alibaba/jstorm/tree/master/jstorm-utility/transaction_meta_spout) for details
