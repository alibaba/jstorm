#!/bin/bash

jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology  detail.yaml --exclude-jars slf4j-log4j
