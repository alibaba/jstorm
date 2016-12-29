# flux
flux是帮助用户更方便创建及部署Apache Storm流式计算拓扑的编程框架。

## 定义
**flux** |fləks| 发音

1. 流动
2. 不断变化
3. 物理上称流动率
4. 助熔剂

## flux生成背景
当拓扑里头的配置是hard-coded，其实很糟糕。一旦我们需要改变拓扑里头的相应配置，我们就必须重新编译和打包。


## 关于
Flux是帮助创建和部署storm拓扑的编程框架及通用组件。

你之前创建拓扑的时候老是会写这样一段代码?:

```java

public static void main(String[] args) throws Exception {
    // logic to determine if we're running locally or not...
    // create necessary config options...
    boolean runLocal = shouldRunLocal();
    if(runLocal){
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, topology);
    } else {
        StormSubmitter.submitTopology(name, conf, topology);
    }
}
```

你有没有想过用以下一条语句就能代替上面，直接创建及提交一个任务呢？

```bash
storm jar mytopology.jar com.alibaba.jstorm.flux.Flux --local config.yaml
```

或者:

```bash
storm jar mytopology.jar com.alibaba.jstorm.flux.Flux --remote config.yaml
```

另外一个痛点就是我们需要在代码里头自己去实现拓扑图构造，即每个spout/bolt的输入和输出.一旦节点有任何变化的时候，我们就得重新编译、打包再提交。
所以Flux的存在，就是为了避免上述重复性工作，使得你完全可以依靠外部文件来定义拓扑图及配置你的拓扑。

## Flux特点

 * 更加简单的创建和部署拓扑，使得你的拓扑代码里头尽量不要嵌入配置信息；
 * 兼容在此之前你已经创建好的拓扑代码；
 * 支持用灵活的.yaml文件来定义storm的api函数(Spouts/Bolts)；
 * .yaml文件也支持对大部分storm组件，storm-kafka, storm-hdfs, storm-hbase等等（jstorm后期会逐渐支持）；
 * 支持多语言；
 * 支持在日常/预发/线上环境自动切换(类似于 Maven `${variable.name}` 替换)；

## 用法

为了使用Flux， 你需要在你的拓扑pom文件依赖它，一起打包；然后通过.yaml配置文件来定义的拓扑。（具体.yaml配置参数下面会介绍）

### 编译Flux
如果你想要编译源码并且跑下单测的话，你需要在本机安装：

* Python 2.6.x or later
* Node.js 0.10.x or later

#### 编译源码，并且需要跑单测:

```
mvn clean install
```

#### 编译源码，但是避免跑单测:
```
mvn clean install -DskipTests=true
```

备注： 如果你想在分布式环境下使用Flux来创建拓扑的话，你仍然需要在集群里头安装python。


#### 编译源码，且需要跑集成测试:

```
mvn clean install -DskipIntegration=false
```
#### pom文件依赖flux
```xml
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>flux-core</artifactId>
    <version>${jstorm.version}</version>
</dependency>
```

#### 创建一个依赖flux的拓扑的pom文件示例
你可以用maven shade或者maven assembly插件来打包，我们这里就以maven shade举例说明：

 ```xml
<!-- include Flux and user dependencies in the shaded jar -->
<dependencies>
    <!-- Flux include -->
    <dependency>
        <groupId>com.alibaba.jstorm</groupId>
        <artifactId>flux-core</artifactId>
        <version>${jstorm.version}</version>
    </dependency>

    <!-- add user dependencies here... -->

</dependencies>
<!-- create a fat jar that includes all dependencies -->
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>1.4</version>
            <configuration>
                <createDependencyReducedPom>true</createDependencyReducedPom>
            </configuration>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <transformers>
                            <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>com.alibaba.jstorm.flux.Flux</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
 ```

### 发布和提交Flux topology
一旦你用flux完成了topology打包，你就可以利用配置文件来跑各种拓扑啦。比如你的jar名称为`myTopology-0.1.0-SNAPSHOT.jar`， 你可以利用以下
命令跑本地模式，当然你也可以跑分布式模式。

```bash
storm jar myTopology-0.1.0-SNAPSHOT.jar com.alibaba.jstorm.flux.Flux --local my_config.yaml

```

### 命令行选项
```
usage: storm jar <my_topology_uber_jar.jar> com.alibaba.jstorm.flux.Flux
             [options] <topology-config.yaml>
 -d,--dry-run                 Do not run or deploy the topology. Just
                              build, validate, and print information about
                              the topology. 
 -e,--env-filter              Perform environment variable substitution.
                              Replace keys identified with `${ENV-[NAME]}`
                              will be replaced with the corresponding
                              `NAME` environment value
 -f,--filter <file>           Perform property substitution. Use the
                              specified file as a source of properties,
                              and replace keys identified with {$[property
                              name]} with the value defined in the
                              properties file.
 -i,--inactive                Deploy the topology, but do not activate it.
 -l,--local                   Run the topology in local mode.
 -n,--no-splash               Suppress the printing of the splash screen.
 -q,--no-detail               Suppress the printing of topology details.
 -r,--remote                  Deploy the topology to a remote cluster.
 -R,--resource                Treat the supplied path as a classpath
                              resource instead of a file.
 -s,--sleep <ms>              When running locally, the amount of time to
                              sleep (in ms.) before killing the topology
                              and shutting down the local cluster.
 -z,--zookeeper <host:port>   When running in local mode, use the
                              ZooKeeper at the specified <host>:<port>
                              instead of the in-process ZooKeeper.
                              (requires Storm 0.9.3 or later) （jstorm暂时不支持）
```

**NOTE:** Flux的命令行选项是不和jstorm自身支持的命令行选项冲突的.

### 具体实例
```
███████╗██╗     ██╗   ██╗██╗  ██╗
██╔════╝██║     ██║   ██║╚██╗██╔╝
█████╗  ██║     ██║   ██║ ╚███╔╝
██╔══╝  ██║     ██║   ██║ ██╔██╗
██║     ███████╗╚██████╔╝██╔╝ ██╗
╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝
+-         Apache Storm        -+
+-  data FLow User eXperience  -+
Version: 0.3.0
Parsing file: /Users/hsimpson/Projects/donut_domination/storm/shell_test.yaml
---------- TOPOLOGY DETAILS ----------
Name: shell-topology
--------------- SPOUTS ---------------
sentence-spout[1](com.alibaba.jstorm.flux.spouts.GenericShellSpout)
---------------- BOLTS ---------------
splitsentence[1](com.alibaba.jstorm.flux.bolts.GenericShellBolt)
log[1](com.alibaba.jstorm.flux.wrappers.bolts.LogInfoBolt)
count[1](com.alibaba.jstorm.testing.TestWordCounter)
--------------- STREAMS ---------------
sentence-spout --SHUFFLE--> splitsentence
splitsentence --FIELDS--> count
count --SHUFFLE--> log
--------------------------------------
Submitting topology: 'shell-topology' to remote cluster...
```

## YAML 配置
Flux 拓扑是通过yaml文件描述和定义的，yaml的文件描述应该依次遵循以下规则：

  1. 拓扑名字；
  2. 定义一些java 对象，可能会在后续定义拓扑过程中被引用，是可选项；
  3. **二选一** (用DSL描述的拓扑):
      * 一些spouts, 每个spout有唯一的ID；
      * 一些bolts， 每个bolt有唯一的ID;
      * 一些streams， 这些streams表示在spouts/botls之间的数据流；
  4. **或者** (能生成类似 `com.alibaba.jstorm.generated.StormTopology` 拓扑的jvm class:
      * 由 `topologySource` 定义.)



这里是一个用yaml DSL描述的简单的wordcount topology例子：

```yaml
name: "yaml-topology"
config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "spout-1"
    className: "com.alibaba.jstorm.testing.TestWordSpout"
    parallelism: 1

# bolt definitions
bolts:
  - id: "bolt-1"
    className: "com.alibaba.jstorm.testing.TestWordCounter"
    parallelism: 1
  - id: "bolt-2"
    className: "com.alibaba.jstorm.flux.wrappers.bolts.LogInfoBolt"
    parallelism: 1

#stream definitions
streams:
  - name: "spout-1 --> bolt-1" # name isn't used (placeholder for logging, UI, etc.)
    from: "spout-1"
    to: "bolt-1"
    grouping:
      type: FIELDS
      args: ["word"]

  - name: "bolt-1 --> bolt2"
    from: "bolt-1"
    to: "bolt-2"
    grouping:
      type: SHUFFLE


```
## yaml属性替换

开发人员经常希望能轻松切换一些的配置，如对主机名、端口和并行参数等配置更改，在部署拓扑时对开发环境/线上环境进行自动切换……这都可以通过使用单独
的YAML配置完成。

Flux允许你把需要经常变更的参数写到 `.properties`文件里头，使得`.yaml` 文件的内容在被解析之前会被`.properties`文件内容替换。使用这个功能
你只要在命令行选项`--filter`后面加上指定`.properties`文件。就像这样一样：

```bash
storm jar myTopology-0.1.0-SNAPSHOT.jar com.alibaba.jstorm.flux.Flux --local my_config.yaml --filter dev.properties
```
其中`dev.properties` 文件内容如下:

```properties
kafka.zookeeper.hosts: localhost:2181
```

你可以根据在`.yaml` 文件中通过 `${}` 指定的key值，在`.properties`文件中找到对应的value，并完成替换:

```yaml
  - id: "zkHosts"
    className: "com.alibaba.jstorm.kafka.ZkHosts"
    constructorArgs:
      - "${kafka.zookeeper.hosts}"
```

在上面的例子中, Flux 用 `localhost:2181` 替换了.yaml文件 `${kafka.zookeeper.hosts}` 的值.

### 环境变量的替换
Flux 也支持对环境变量的更改及替换. 比如系统拥有环境变量 `ZK_HOSTS` , 如果你想用同样的方式更改该变量的值得话，那么你需要在.yaml按照如下的
语法引用该变量：
```
${ENV-ZK_HOSTS}
```

## 实例对象
我们可以用配置文件的方式生成一些对象，这些对象可能会被spouts/bolts应用。利用配置来生成实例对象的方式类似于Spring beans。
每个实例对象配置主要包括了唯一性ID以及className。我们想创建`com.alibaba.jstorm.kafka.StringScheme` 实例，同时该class有无参数构造
函数的话，那么我们可以在.yaml文件这么定义：
```yaml
components:
  - id: "stringScheme"
    className: "com.alibaba.jstorm.kafka.StringScheme"
```

### 构造函数, 引用, 属性和方法 

####构造函数
构造函数的形参可以通过`contructorArgs`来指定，`contructorArgs`是一个list，将会被传递到构造函数用来创建对象。比如下面的创建一个zkHosts
对象：

```yaml
  - id: "zkHosts"
    className: "com.alibaba.jstorm.kafka.ZkHosts"
    constructorArgs:
      - "localhost:2181"
      - true
```

####引用
每一个实例对象生成都必须指定一个唯一性ID，这样的话我们就可以在其他实例对象的创建或者调用过程中，利用该ID达到对其对应的实例引用。但是注意的是
要在对象B中引用A对象的话，那么应该在定义创建B对象的配置文件中，用`ref` 来标识对象A所对应ID。
下面的这个例子，就是一个实例对象率先被创建，其ID是`"stringScheme"` ，过后另外一个对象的构造函数要用到StringScheme作为形参，那么它就需要
在`contructorArgs`中用`ref` 来标识`"stringScheme"`。

```yaml
components:
  - id: "stringScheme"
    className: "com.alibaba.jstorm.kafka.StringScheme"

  - id: "stringMultiScheme"
    className: "com.alibaba.jstorm.spout.SchemeAsMultiScheme"
    constructorArgs:
      - ref: "stringScheme" # component with id "stringScheme" must be declared above.
```
**N.B.:** References can only be used after (below) the object they point to has been declared.

####属性
除了调用构造函数来初始化对象的一些属性以外， Flux还支持用户可以通过配置文件来设置对象属性的值，前提是这些属性都有一个set方法，及该方法都是
`public`：

```yaml
  - id: "spoutConfig"
    className: "com.alibaba.jstorm.kafka.SpoutConfig"
    constructorArgs:
      # brokerHosts
      - ref: "zkHosts"
      # topic
      - "myKafkaTopic"
      # zkRoot
      - "/kafkaSpout"
      # id
      - "myId"
    properties:
      - name: "forceFromStart"
        value: true
      - name: "scheme"
        ref: "stringMultiScheme"
```

在外面的例子中，首先构造了一个spoutConfig实例，然后Flux会先去该实例查找方法`setForceFromStart(boolean b)`，如果找到就会把true设置进去；
如果没有找到，Flux就继续在public属性集中查找`forceFromStart`属性，找到的话会将true赋予它。
In the example above, the `properties` declaration will cause Flux to look for a public method in the `SpoutConfig` with
the signature `setForceFromStart(boolean b)` and attempt to invoke it. If a setter method is not found, Flux will then
look for a public instance variable with the name `forceFromStart` and attempt to set its value.

References may also be used as property values.
备注： 引用的方式也支持在yaml属性替换中使用

####配置方法调用
在配置文件中配置对象的一些方法后，就自动会在实例化对象后，马上去调用这些方法。比如你不想通过构造函数的方式或者直接赋值的方式来设置某些属性的话，
可以自定义一些方法来设置这些属性，这样做可以避免暴露一些属性的名称。

以下就是一个例子用.yaml创建一系列bolts，并在创建完bolt后会主动调用指定的几个方法：

```yaml
bolts:
  - id: "bolt-1"
    className: "com.alibaba.jstorm.flux.test.TestBolt"
    parallelism: 1
    configMethods:
      - name: "withFoo"
        args:
          - "foo"
      - name: "withNone"
      # no args needed, so no "args" line
      - name: "withBar"
        args:
          - "bar"
      - name: "withFooBar"
        args:
          - "foo"
          - "bar"
```

TestBolt 这些方法声明如下:

```java
    public void withFoo(String foo);
    public void withNone(); // method with zero arguments
    public void withBar(String bar);
    public void withFooBar(String foo, String bar);
```
这些方法的形参配置和也支持引用方式的。

### 支持java类型 `enum`

比如, [Storm's HDFS module]() 包括了 `enum`定义:

```java
public static enum Units {
    KB, MB, GB, TB
}
```

在 `com.alibaba.jstorm.hdfs.bolt.rotation.FileSizeRotationPolicy` 中有这样的构造函数:

```java
public FileSizeRotationPolicy(float count, Units units)

```
那么你可以像下面一样配置，来创建FileSizeRotationPolicy对象

```yaml
  - id: "rotationPolicy"
    className: "com.alibaba.jstorm.hdfs.bolt.rotation.FileSizeRotationPolicy"
    constructorArgs:
      - 5.0
      - MB
```

上面的配置和我们直接通过java代码创建对象的过程是一样的

```java
// rotate files when they reach 5MB
FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
```

## Topology Config
`config` 是一个`com.alibaba.jstorm.Config` 对象，类似于Map。当我们提交拓扑的时候，这个config会被传递给
`com.alibaba.jstorm.StormSubmitter` 用来提交拓扑。我们可以在配置文件中，这么定义来创建config对象的。

```yaml
config:
  topology.workers: 4
  topology.max.spout.pending: 1000
  topology.message.timeout.secs: 30
```

# Flux兼容之前已经创建好的拓扑
如果你在使用Flux之前，就已经写完了拓扑，那么你仍然还是可以使用Flux来部署/运行的。这种情况下，你仍然可以使用Flux的一些功能，如调用构造函数
及public方法来创建对象，利用引用传递和利用配置文件来创建Topology Config。

最简单的使用方法就是在你的topology类中定义`getTopology()`的方法

```java
public StormTopology getTopology(Map<String, Object> config)
```
或者:

```java
public StormTopology getTopology(Config config)
```

那么你就可以利用Flux通过配置文件来更新你拓扑的一些配置，就想这样一样:

```yaml
name: "existing-topology"
topologySource:
  className: "com.alibaba.jstorm.flux.test.SimpleTopology"
```
当然你可以可以在topology 类中定义其他名称的方法(而不是`getTopology`)，那么你的配置文件就得这么写：

```yaml
name: "existing-topology"
topologySource:
  className: "com.alibaba.jstorm.flux.test.SimpleTopology"
  methodName: "getTopologyWithDifferentMethodName"
```

__备注:__ 你自定义个那个方法的形参必须是`java.util.Map<String, Object>` 或者`com.alibaba.jstorm.Config`, 
返回值是 `com.alibaba.jstorm.generated.StormTopology`对象。

# YAML DSL创建拓扑
## Spouts and Bolts 配置
其实Spouts和Bolts的创建是类似于之前提到的实例对象的创建方式，只不过增加了`parallelism`属性

Shell spout example:

```yaml
spouts:
  - id: "sentence-spout"
    className: "com.alibaba.jstorm.flux.spouts.GenericShellSpout"
    # shell spout constructor takes 2 arguments: String[], String[]
    constructorArgs:
      # command line
      - ["node", "randomsentence.js"]
      # output fields
      - ["word"]
    parallelism: 1
```

Kafka spout example:

```yaml
components:
  - id: "stringScheme"
    className: "com.alibaba.jstorm.kafka.StringScheme"

  - id: "stringMultiScheme"
    className: "com.alibaba.jstorm.spout.SchemeAsMultiScheme"
    constructorArgs:
      - ref: "stringScheme"

  - id: "zkHosts"
    className: "com.alibaba.jstorm.kafka.ZkHosts"
    constructorArgs:
      - "localhost:2181"

# Alternative kafka config
#  - id: "kafkaConfig"
#    className: "com.alibaba.jstorm.kafka.KafkaConfig"
#    constructorArgs:
#      # brokerHosts
#      - ref: "zkHosts"
#      # topic
#      - "myKafkaTopic"
#      # clientId (optional)
#      - "myKafkaClientId"

  - id: "spoutConfig"
    className: "com.alibaba.jstorm.kafka.SpoutConfig"
    constructorArgs:
      # brokerHosts
      - ref: "zkHosts"
      # topic
      - "myKafkaTopic"
      # zkRoot
      - "/kafkaSpout"
      # id
      - "myId"
    properties:
      - name: "forceFromStart"
        value: true
      - name: "scheme"
        ref: "stringMultiScheme"

config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "kafka-spout"
    className: "com.alibaba.jstorm.kafka.KafkaSpout"
    constructorArgs:
      - ref: "spoutConfig"

```

Bolt Examples:

```yaml
# bolt definitions
bolts:
  - id: "splitsentence"
    className: "com.alibaba.jstorm.flux.bolts.GenericShellBolt"
    constructorArgs:
      # command line
      - ["python", "splitsentence.py"]
      # output fields
      - ["word"]
    parallelism: 1
    # ...

  - id: "log"
    className: "com.alibaba.jstorm.flux.wrappers.bolts.LogInfoBolt"
    parallelism: 1
    # ...

  - id: "count"
    className: "com.alibaba.jstorm.testing.TestWordCounter"
    parallelism: 1
    # ...
```
## 数据流及Grouping方式的配置
在Flux中数据流表示在拓扑中Spouts/bolts消息传递关系，数据流的定义应该包括以下这些属性：

**`name`:** A name for the connection (optional, currently unused)

**`from`:** The `id` of a Spout or Bolt that is the source (publisher)

**`to`:** The `id` of a Spout or Bolt that is the destination (subscriber)

**`grouping`:** The stream grouping definition for the Stream

A Grouping definition has the following properties:

**`type`:** The type of grouping. One of `ALL`,`CUSTOM`,`DIRECT`,`SHUFFLE`,`LOCAL_OR_SHUFFLE`,`FIELDS`,`GLOBAL`,
 or `NONE`，`LOCAL_FIRST`, `PARTIAL_KEY`.

**`streamId`:** The Storm stream ID (Optional. If unspecified will use the default stream)

**`args`:** For the `FIELDS` grouping, a list of field names.

**`customClass`** For the `CUSTOM` grouping, a definition of custom grouping class instance

The `streams` definition example below sets up a topology with the following wiring:

```
    kafka-spout --> splitsentence --> count --> log
```


```yaml
# stream 定义
# stream 定义了在 spouts 和 bolts直接的连接关系.
# 支持自定义Grouping方式

streams:
  - name: "kafka --> split" # name isn't used (placeholder for logging, UI, etc.)
    from: "kafka-spout"
    to: "splitsentence"
    grouping:
      type: SHUFFLE

  - name: "split --> count"
    from: "splitsentence"
    to: "count"
    grouping:
      type: FIELDS
      args: ["word"]

  - name: "count --> log"
    from: "count"
    to: "log"
    grouping:
      type: SHUFFLE
```

### 自定义 Groupings

自定义Groupings 方式，实际上就是创建一个Grouping实例对象，所以它有支持在配置文件中配置构造函数调用、引用、属性赋值等操作。

下面给出了一个创建`com.alibaba.jstorm.testing.NGrouping`自定义Grouping方式的实例：

```yaml
  - name: "bolt-1 --> bolt2"
    from: "bolt-1"
    to: "bolt-2"
    grouping:
      type: CUSTOM
      customClass:
        className: "com.alibaba.jstorm.testing.NGrouping"
        constructorArgs:
          - 1
```

## 多个.yaml文件

Flux也支持你在主.yaml配置includes其他.yaml文件。这样的话其他.yaml文件的内容会被读取，一起加载到主.yaml文件内容中。

```yaml
includes:
  - resource: false
    file: "src/test/resources/configs/shell_test.yaml"
    override: false
```

如果 `resource` 属性设置为`true`,那么include的yaml文件就在你jar的resource里头，否则的话你就得配置绝对路径。

如果其他.yaml文件内容有和主yaml内容有冲突的话，这时候你配置 `override` 的属性是false的话，那么冲突的内容就不会添加到主.yaml文件中，
如果`override` 的属性是true的话，那么主.yaml文件中冲突的内容就会被覆盖。

**注意.:** Includes 不支持递归. Includes 来自 included 文件会被忽略.


## Basic Word Count 例子

This example uses a spout implemented in JavaScript, a bolt implemented in Python, and a bolt implemented in Java

Topology YAML config:

```yaml
---
name: "shell-topology"
config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "sentence-spout"
    className: "com.alibaba.jstorm.flux.spouts.GenericShellSpout"
    # shell spout constructor takes 2 arguments: String[], String[]
    constructorArgs:
      # command line
      - ["node", "randomsentence.js"]
      # output fields
      - ["word"]
    parallelism: 1

# bolt definitions
bolts:
  - id: "splitsentence"
    className: "com.alibaba.jstorm.flux.bolts.GenericShellBolt"
    constructorArgs:
      # command line
      - ["python", "splitsentence.py"]
      # output fields
      - ["word"]
    parallelism: 1

  - id: "log"
    className: "com.alibaba.jstorm.flux.wrappers.bolts.LogInfoBolt"
    parallelism: 1

  - id: "count"
    className: "com.alibaba.jstorm.testing.TestWordCounter"
    parallelism: 1

#stream definitions
# stream definitions define connections between spouts and bolts.
# note that such connections can be cyclical
# custom stream groupings are also supported

streams:
  - name: "spout --> split" # name isn't used (placeholder for logging, UI, etc.)
    from: "sentence-spout"
    to: "splitsentence"
    grouping:
      type: SHUFFLE

  - name: "split --> count"
    from: "splitsentence"
    to: "count"
    grouping:
      type: FIELDS
      args: ["word"]

  - name: "count --> log"
    from: "count"
    to: "log"
    grouping:
      type: SHUFFLE
```


## Micro-Batching (Trident) API Support

现在Flux YAML DSL 只是支持Core Storm API， 还不支持Trident API.如果你非要在Trident 拓扑中使用Flux的话，那么你只能在写好的拓扑中
定义get 拓扑的方法，就想之前“Flux兼容之前已经创建好的拓扑”介绍的那样去做。

```yaml
name: "my-trident-topology"

config:
  topology.workers: 1

topologySource:
  className: "com.alibaba.jstorm.flux.test.TridentTopologySource"
  # Flux will look for "getTopology", this will override that.
  methodName: "getTopologyWithDifferentMethodName"
```

