---
title: Upgrade JStorm2.1.1 To JStorm2.2.1
is_beta: false

sub-nav-group: Upgrade
sub-nav-id: UpgradeApplication
sub-nav-pos: 3
---

This article is a guide on how to upgrade from JStorm 2.1.1 to JStorm 2.2.1. 

## Upgrade Cluster         
                                  
Please refer to `Upgrade Cluster` document, besides, there're a few new features that need to take care of.   

### Cluster Config Sync

This is a new feature in JStorm 2.2.1. Supervisors will sync the latest storm.yaml from nimbus automatically.
So if you want to change storm.yaml for the whole cluster, just edit storm.yaml in nimbus master, save it,
the new config will be synced to supervisors.
  
However, if you want something special for each supervisor, you need to add a config: `jstorm.yarn.conf.blacklist`.

```
jstorm.yarn.conf.blacklist: "jstorm.log.dir, jstorm.home"
```

Still, there's a known issue, for complex data types, e.g., supervisor port list: 

```
supervisor.slots.ports:
    -6800
    -6801
```

For complex data types like list or map, you need to convert the value to standard yaml, in the above case, change 
port list value to: 

```
supervisor.slots.ports: [6800,6801]
```

Meanwhile, you can customize your own `nimbus.config.update.handler.class`, which implements `ConfigUpdateHandler` 
interface, then you can change your cluster config in other ways, e.g., an external config center. 

Still, please note that, cluster config sync doesn't mean the new config will take effect automatically.
 You still need to restart nimbus and supervisor, currently.


### External Modules

Starting from 2.2.1, nimbus and supervisor support external modules.
e.g., you've developed an implementation of metric uploader and plan to plugin it to nimbus, then you'll need to 
follow below steps: 

1. code and package your plugin jar (possibly with a bunch of dependency jars)

2. create dir: {jstorm.home}/lib/ext, cd ext and create a sub dir: metric_plugin

3. copy your jar and dependencies into metric_plugin。

4. change storm.yaml, add config: `nimbus.external: metric_plugin`

5. restart nimbus, after that, nimbus will add metric_plugin/* to its classpath, then you can access 
your plugin classes and its features.

Supervisor supports external modules as well, please use `supervisor.external` config.


## Code Changes

Upgrading from 2.1.1 to 2.2.1 is mostly compatible on application level, just a few minor nits.

a. change jstorm-core pom dependency:

```
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.2.1</version>
    <scope>provided</scope>
</dependency>
```

c. re-compile. you may encounter errors in this step. e.g., if you use user defined metrics, there might be a few 
  package rename, in this case, just remove the original imports and re-import.

d. this step is **very important**, starting from jstorm 2.2.1, jstorm-core has shaded some dependencies which may 
cause problems like snakeyaml，curator-framework, etc.
So if your application depends on snakeyaml to parse yaml, or uses curator framework, you need to add these
  dependencies yourself, otherwise you may encounter errors like "ClassNotFoundError".

## About serialization

Starting from 2.2.1, jstorm enables kryo by default, which requires no-arg constructor for objects. 

IMPORTANT NOTICE(3 times):

```
objects with no-arg constructor
objects with no-arg constructor
objects with no-arg constructor
```

Otherwise you may get error like:

```
Exception in thread "main" com.esotericsoftware.kryo.KryoException: Class cannot be created (missing no-arg constructor): java.util.List

```

If, unfortunately, you can't make a no-arg constructor for your objects, say, the objects are in your dependencies, 
there're 2 ways to fix:

### disable kryo

by adding config to your topology conf to disable kryo:

```
topology.fall.back.on.java.serialization: true
```

you can fall back to java serialization.


### create a custom serializer

If performance is a crucial concern, you may not want java serialization, thus custom 
serializer opts in.

For details, please refer to `PairSerializer` class within sequence-split-merge example in source code.

For example, ByteBuffer, doesn't have a no-arg constructor, and it's an abstract class, but we want a serializer for 
`HeapByteBuffer` class, that's how we do:

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

After that, register the serializer to kryo:

```
Config.registerSerialization(stormConf, "java.nio.HeapByteBuffer", KryoByteBufferSerializer.class);
```

## Logging Systems

Please refer to the doc on upgrading JStorm 0.9.x to JStorm 2.1.1

## About Classloader

In earlier versions, if we have dependency conflicts that can't be resolved, we need to enable classloader.
However, as mentioned above, JStorm 2.2.1 has shaded some dependencies, in most cases, classloader will not be
necessary and it's recommended, if, you have enabled classloader before, turn it off.

