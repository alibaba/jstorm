#!/bin/bash

jstorm jar sequence-split-merge.jar com.alipay.dw.jstorm.example.drpc.ReachTopology --exclude-jars slf4j-log4j simple.yaml
