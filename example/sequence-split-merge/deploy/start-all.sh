#!/bin/bash

jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology detail.yaml --exclude-jars slf4j-log4j 
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.batch.SimpleBatchTopology simple.yaml --exclude-jars slf4j-log4j -c "topology.debug.recv.tuple=true,topology.debug=true"



#"  -conf <conf.xml>                            load configurations from",
#"                                              <conf.xml>", "  -conf <conf.yaml>                           load configurations from",
#"                                              <conf.yaml>",
#"  -D <key>=<value>                            set <key> in configuration",
#"                                              to <value> (preserve value's type)",
#"  -libjars <comma separated list of jars>     specify comma separated",
#"                                              jars to be used by",
#"                                              the submitted topology",
jstorm kill SequenceTest 1
sleep 10
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.sequence.SequenceTopologyTool detail.yaml --exclude-jars slf4j-log4j  -conf detail.yaml -D "topology.name=SequenceTopologyTool,topology.debug=true" 



jstorm jar sequence-split-merge.jar org.apache.storm.starter.trident.TridentMapExample --exclude-jars slf4j-log4j
jstorm jar sequence-split-merge.jar org.apache.storm.starter.trident.TridentMinMaxOfDevicesTopology
jstorm jar sequence-split-merge.jar org.apache.storm.starter.trident.TridentMinMaxOfVehiclesTopology
jstorm jar sequence-split-merge.jar org.apache.storm.starter.trident.TridentWordCount
jstorm jar sequence-split-merge.jar org.apache.storm.starter.InOrderDeliveryTest simple.yaml


jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology  --exclude-jars slf4j-log4j
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.multilanguage.WordCountTopology remote
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.userdefined.scheduler.TaskInDifferentNodeTopology
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology  simple.yaml
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.userdefined.scheduler.TaskInDifferentNodeTopology simple.yaml
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.userdefined.scheduler.UserDefinedWorkerTopology
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.userdefined.scheduler.UserDefinedHostsTopology
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.userdefined.scheduler.UserDefinedWorkerTopology simple.yaml
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.window.RollingTopWords
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.window.SkewedRollingTopWords
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.window.SlidingTupleTsTopology
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.window.SlidingWindowTopology
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.transcation.TransactionalGlobalCount
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.transcation.TransactionalWords
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.transcation.TransactionTestTopology simple.yaml
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.update.topology.TestBackpressure











