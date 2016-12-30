---
title:  "Example"
# Top-level navigation
top-nav-group: QuickStart
top-nav-pos: 2
top-nav-id: Example
top-nav-title: Example
layout: plain
---

* This will be replaced by the TOC
{:toc}

This page helps readers to quickly implement a JStorm example.

[Example source](https://github.com/alibaba/jstorm/tree/master/example/sequence-split-merge)

***

# Step by step
The simplest JStorm example is divided into four steps:

## **Generate Topology**

```
Map conf = new HashMap();
//all custom configurations of topology are placed in the Map

TopologyBuilder builder = new TopologyBuilder();
//create topology builder

int spoutParal = get("spout.parallel", 1);
//get spout concurrency settings

SpoutDeclarer spout = builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
                new SequenceSpout(), spoutParal);
//create Spout， new SequenceSpout() is spout object,
//SequenceTopologyDef.SEQUENCE_SPOUT_NAME is spout name, note that name do not contain space

int boltParal = get("bolt.parallel", 1);
//get bolt concurrency settings

BoltDeclarer totalBolt = builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME, new TotalCount(),
                boltParal).shuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
//create bolt, SequenceTopologyDef.TOTAL_BOLT_NAME is bolt name,
// TotalCount is bolt object，boltParal is the number of bolt concurrent,
//shuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME)， 
//receive the data of SequenceTopologyDef.SEQUENCE_SPOUT_NAME by shuffle，
//That is, each spout random polling send tuple to the next bolt

int ackerParal = get("acker.parallel", 1);
Config.setNumAckers(conf, ackerParal);
//Set the The number of acker concurrent

int workerNum = get("worker.num", 10);
conf.put(Config.TOPOLOGY_WORKERS, workerNum);
//indicates the number of worker topology to be used

conf.put(Config.STORM_CLUSTER_MODE, "distributed");
//set topology model for distributed，this topology can put runs on a JStorm cluster

StormSubmitter.submitTopology(streamName, conf, builder.createTopology());
//submit topology
```

## **IRichSpout**

IRichSpout is the easiest Spout Interface

```
 IRichSpout{

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
```

Note:
* Spout object must implement Serializable interface, requiring all the fields within the spout must be serializable, if there are some fileds which are not Serialable, please add "transient" attribute.
* Spout can have a constructor function, but the constructor will be executed only once when the topology is being submitted; At this time the spout object has been created; Therefore, some initialization can be done here before task is assigned to a worker, once constructor has completed, the contents of the initialization will be brought to each task (because when you submit a topology, spout object will be serialized into a file, then the file will be delivered to supervisors, at last spout object will be deserialized from the file when worker starts).
* open action will be called when the worker containing the task has been started.
* close action will be called when the task begin to shutdown.
* activate action will be triggered after the topology is activated.
* deactivate action will be triggered after the topology is deactivated.
* nextTuple is the core function of a spout, all data had better be emitted in this function.
* ack action will be triggered after the message has been done in the whole topology, please refer to [Acking Framework](https://github.com/alibaba/jstorm/wiki/Acking-Framework-Implementation) for details.
* fail action will be triggered after the message fail to be done or timeout, please refer to [Acking Framework](https://github.com/alibaba/jstorm/wiki/Acking-Framework-Implementation) for details.
* declareOutputFields defines the meaning of each field of tuple emitted by spout.
* getComponentConfiguration is the interface of component configuration for spout, spout can own himself configuration with the interface.

## **Bolt**

```
IRichBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
```

Note:
* Bolt object must implement Serializable interface, requiring all the fields within the bolt must be serializable, if there are some fileds which are not Serialable, please add "transient" attribute.
* Bolt can have a constructor function, but the constructor will be called only once when the topology is being submitted; At this time the bolt object has been created; Therefore, some initialization can be done here before task is assigned to a worker, once constructor has finished, the contents of the initialization will be brought to each task (because when you submit a topology, bolt object will be serialized into a file, then the file will be delivered to supervisors, at last bolt object will be deserialized from the file when worker starts).
* prepare action does the initialization action when the worker containing the task has been started.
* cleanup action will be called when the task begins to shutdown.
* execute is the core function of a blot. In the function, when bolt process a message and emit a message if needed, bolt need to call collector.ack, when bolt is unable to process a message or have an error, you need to call collector.fail, please refer to [Acking Framework](https://github.com/alibaba/jstorm/wiki/Acking-Framework-Implementation) for details.
* declareOutputFields defines the meaning of each field which emitted by bolt.
* getComponentConfiguration is the interface of component configuration for bolt, bolt can own himself configuration with the interface.

## **Compile**

Configuration in Maven pom.xml

```
    <dependency>
        <groupId>com.alibaba.jstorm</groupId>
        <artifactId>jstorm-client</artifactId>
        <version>0.9.6.0</version>
        <scope>provided</scope>
     </dependency> 
         <dependency>
            <groupId>com.alibaba.jstorm</groupId>
            <artifactId>jstorm-client-extension</artifactId>
            <version>0.9.6.0</version>
            <scope>provided</scope>
        </dependency>
```

If you can not find jstorm-client and jstorm-client-extension package, you can download the source code of JStorm to compile, please refer to the [Build-JStorm](https://github.com/alibaba/jstorm/wiki/Build-JStorm)

When compile, you need package all dependent jars to one package.

```
<build>
        <plugins>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>1.7.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                            <!-- 
                                <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>META-INF/spring.handlers</resource>
                                </transformer>
                                <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>META-INF/spring.schemas</resource>
                                </transformer>
                                <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>${Please set the application main entrance class, such as:com.alibaba.aloha.pandora.PandoraTopology}</mainClass>
                                </transformer>
                                 -->
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```

## **Submit jar**

jstorm jar xxxxxx.jar com.xxxx.xxxx.xx parameter
* xxxx.jar is packaged jar.
* com.xxx.xxxx.xx is the entry of the jar.
* parameter is the arguments of topology.
