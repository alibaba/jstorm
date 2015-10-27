#!/bin/bash

#jstorm jar target/sequence-split-merge-1.1.0-jar-with-dependencies.jar com.alipay.dw.jstorm.transcation.TransactionalGlobalCount global
jstorm jar target/sequence-split-merge-1.1.0-jar-with-dependencies.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology conf/conf.yaml
#jstorm jar target/sequence-split-merge-1.0.8-jar-with-dependencies.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology conf/conf.prop
#jstorm jar target/sequence-split-merge-1.0.8-jar-with-dependencies.jar com.alipay.dw.jstorm.example.batch.SimpleBatchTopology conf/topology.yaml
jstorm jar sequence-split-merge-1.1.0-jar-with-dependencies.jar com.alipay.dw.jstorm.example.sequence.KafkaMontoringTopology conf.yaml
jstorm jar sequence-split-merge-1.1.0.jar com.alipay.dw.jstorm.example.sequence.KafkaMontoringTopology conf.yaml

#kafkaMontoring depends on belowing jar, copy from kafka/lib and kafka-manager/lib
#com.yammer.metrics.metrics-core-2.2.0.jar
#kafka_2.11-0.8.2.2.jar
#kafka-clients-0.8.2.2.jar
#scala-library-2.11.5.jar
#scala-parser-combinators_2.11-1.0.2.jar
#scala-xml_2.11-1.0.2.jar
#jstorm-kafka-2.0.4-SNAPSHOT.jar
