#!/bin/bash

jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology detail.yaml --exclude-jars slf4j-log4j 
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.batch.SimpleBatchTopology simple.yaml --exclude-jars slf4j-log4j -c "topology.debug.recv.tuple=true,topology.debug=true"
jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.trident.TridentWordCount TridentWordCount --exclude-jars slf4j-log4j

