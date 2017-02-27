---
title: 从JStorm2.1.1升级到JStorm2.2.1
is_beta: false

sub-nav-group: Upgrade_cn
sub-nav-id: UpgradeApplication_cn
sub-nav-pos: 4
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}


本文主要讲述如何从2.1.1升级到2.2.1。

## 集群重新安装
参见集群升级指南，几点需要注意的

### 配置自动同步
2.2.1添加了一个新功能：配置自动同步，这意味着，supervisor会自动从nimbus同步最新的storm.yaml。所以，如果你想要同步集群的配置，
只需要到nimbus master上修改一下storm.yaml，保存，配置就会被同步到supervisor。但是如果你想要每个supervisor都有一些不同的配置，
那需要加一个配置：`jstorm.yarn.conf.blacklist`。如：

```
jstorm.yarn.conf.blacklist: "jstorm.log.dir, jstorm.home"
```
但是目前的配置同步有一个问题，对于复杂类型的，比如port list的：

```
supervisor.slots.ports:
    -6800
    -6801
```

如果在conf blacklist中设置了这种复杂值类型的配置项，那么在同步的时候可能会出现问题。这时需要你修改一下配置，把yaml中的值改成list类型的值，即：

```
supervisor.slots.ports: [6800,6801]
```
同时，你也可以自定义你的`nimbus.config.update.handler.class`。只需实现`ConfigUpdateHandler`接口即可。这样可以通过其他的方式来动态修改集群
的配置，如通过外部的配置中心修改特定集群的配置。

最后**注意**：配置的自动更新，并不意味着会自动生效。目前来说，还是需要重启nimbus或supervisor才能让大部分的配置自动生效。


### ext模块
2.2.1开始，nimbus和supervisor支持加载external library。举例来说，你写了一个metric uploader的实现，要作为插件plugin到nimbus中。那么：

1. 首先，你写好代码，为这个插件打出一个单独的jar包（或者可能还有一堆第三方依赖包）。

2. 创建目录：{jstorm.home}/lib/ext，然后进入ext目录，创建一个子目录，如: metric_plugin。

3. 把你的jar及依赖复制到metric_plugin。

4. 修改storm.yaml，加入配置：`nimbus.external: metric_plugin`。

5. 重启nimbus，这时nimbus启动的时候就会自动把metric_plugin/*加入到它的classpath中，这样就能访问到你的插件类及其功能了。
同样的，supervisor也支持supervisor.external功能。


## 代码修改

2.1.1到2.2.1代码改动相对较小，大部分是兼容的，只需要少许修改即可完成升级。

a. 添加jstorm-core依赖：

```
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.2.1</version>
    <scope>provided</scope>
</dependency>
```

c. 重新编译。这一步可能会出错，因为如果你用了自定义metrics，很可能在2.2.1中，package名或者接口名都已经发生了变化，
你需要把原来的package依赖删了，重新import一下。

d. 接下来这一步比较**重要**，从jstorm 2.2.1开始，jstorm-core内部对许多容易产生冲突的包做了shade，如snakeyaml，curator-framework等。
如果你的应用依赖了snakeyaml做yaml的解析，或者用了curator framework，并且之前是依赖于jstorm的版本的，即，应用自己的代码中没有直接或者间接
加上这个依赖，那么在升级到2.2.1之后，有可能会出现相关的类找不到，这时需要你在应用代码中手动加上这些依赖。

## 关于序列化
从2.2.1开始，jstorm默认开启了kryo序列化，这要求传输的对象（及其所有非static及transient变量）需要有无参构造函数

重复3遍

```
所有传输对象 含有无参构造函数
所有传输对象 含有无参构造函数
所有传输对象 含有无参构造函数
```

否则可能会抛出类似于下面的异常：

```
Exception in thread "main" com.esotericsoftware.kryo.KryoException: Class cannot be created (missing no-arg constructor): java.util.List

```

当无法为所有对象创建出无参构造函数时，有两种解决方法：

### 关闭kryo

在conf中添加如下配置可以做到：

```
topology.fall.back.on.java.serialization: true
```
这样就会使用java的默认序列化方式

### 自定义序列化器

如果对性能要求比较高，你可能并不想这么做，那么就需要为你的类实现一个自定义的kryo序列化器。
可以参考源码中sequence-split-merge代码中`PairSerializer`类。

举例来说，ByteBuffer是没有无参构造函数的(包括HeapByteBuffer)，而且是个抽象类，我们自定义的ByteBufferSerializer代码如下：

```java
public class KryoByteBufferSerializer extends Serializer<ByteBuffer> {

    @Override
    public void write(Kryo kryo, Output output, ByteBuffer object) {
        output.writeInt(object.array().length);
        output.write(object.array());
    }

    @Override
    public ByteBuffer read(Kryo kryo, Input input, Class<ByteBuffer> type) {
        int len = input.readInt();
        return ByteBuffer.wrap(input.readBytes(len));
    }
}
```

然后把serializer注册到kryo：

```
Config.registerSerialization(stormConf, "java.nio.HeapByteBuffer", KryoByteBufferSerializer.class);
```

## 日志系统

请参考0.9.x升级到2.x的文档

## 关于classloader

上面已经提到，jstorm-core对容易冲突的依赖做了shade，因此基本上不再需要开启classloader来解决。
如果你之前有应用开启过classloader，那么可以考虑在升级到2.2.1之后，关掉classloader。
