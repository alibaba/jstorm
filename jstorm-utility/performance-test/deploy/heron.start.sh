#!/usr/bin/env bash

heron submit local performance-test-heron-1.0.jar com.alibaba.jstorm.performance.test.PerformanceTestTopology simple.yaml
heron submit local performance-test-heron-1.0.jar com.alibaba.jstorm.performance.test.FastWordCountTopology simple.yaml