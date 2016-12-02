---
title: "HDFS 插件"
layout: plain_cn

# Sub navigation
sub-nav-parent: Plugins_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: HDFS_cn
#sub-nav-pos: 1
sub-nav-title: HDFS 插件
---

* This will be replaced by the TOC
{:toc}

# Storm HDFS

Storm HDFS 组件

 - HDFS Bolt 
 - HDFS Spout

---

# HDFS Bolt

## 用法
以下例子是将分隔符("|")写到路径hdfs://localhost:54310/foo的HDFS系统里去。每发送了1000条这样的消息后，会做一次HDFS文件系统同步，这样
文件会对所有的HDFS客户端可见。如果存储的文件大小到达5MB时，文件会自动回滚。

```java
// use "|" instead of "," for field delimiter
RecordFormat format = new DelimitedRecordFormat()
        .withFieldDelimiter("|");

// sync the filesystem after every 1k tuples
SyncPolicy syncPolicy = new CountSyncPolicy(1000);

// rotate files when they reach 5MB
FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

FileNameFormat fileNameFormat = new DefaultFileNameFormat()
        .withPath("/foo/");

HdfsBolt bolt = new HdfsBolt()
        .withFsUrl("hdfs://localhost:54310")
        .withFileNameFormat(fileNameFormat)
        .withRecordFormat(format)
        .withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy);
```


### Packaging a Topology
建议你用[maven-shade-plugin]()插件来打包Topology.因为shade插件打包时可以合并多个JAR manifest文件，这些文件对hadloop client至关重要。
一般如果出现这样的错误：

```
java.lang.RuntimeException: Error preparing HdfsBolt: No FileSystem for scheme: hdfs
```

说明可能是你打包方式不正确，检查下你的pom文件是不是这样的。

```xml
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
                        <mainClass></mainClass>
                    </transformer>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>

```

### 指定Hadoop版本
默认使用以下Hadoop配置

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.6.1</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-hdfs</artifactId>
    <version>2.6.1</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```
如果你打算依赖其他版本的Hadoop，你记得在storm-hdfs里头把冲突的架包排除。当Hadoop client客户端版本冲突时一般会有这样的错误：

```
com.google.protobuf.InvalidProtocolBufferException: Protocol message contained an invalid tag (zero)
```

## 自定义 HDFS Bolt

### Tuple转换成byte []
我们提供了Record Format转换的接口，以便将Tuple转换成 byte [],以便能导入HDFS.
`org.apache.storm.hdfs.format.RecordFormat`

```java
public interface RecordFormat extends Serializable {
    byte[] format(Tuple tuple);
}
```
此外我们自己实现了将Tuple转换CSV和tab-delimited流的接口，具体请参见`org.apache.storm.hdfs.format.DelimitedRecordFormat`。


### 文件命名
我们提供了文件命名的接口`org.apache.storm.hdfs.format.FileNameFormat`

```java
public interface FileNameFormat extends Serializable {
    void prepare(Map conf, TopologyContext topologyContext);
    String getName(long rotation, long timeStamp);
    String getPath();
}
```

默认的文件命名的实现请参考`org.apache.storm.hdfs.format.DefaultFileNameFormat`  :

     {prefix}{componentId}-{taskId}-{rotationNum}-{timestamp}{extension}

比如:

     MyBolt-5-7-1390579837830.txt

默认, prefix 是空的， extenstion 是 ".txt".



### 同步策略

我们提供了同步策略的接口，同步策略可以让你可以控制缓冲数据刷新到底层文件系统的时机，同步后的数据将对所有客户端可见。
`org.apache.storm.hdfs.sync.SyncPolicy` 

```java
public interface SyncPolicy extends Serializable {
    boolean mark(Tuple tuple, long offset);
    void reset();
}
```
每发送一条消息，`HdfsBolt` 将调用 `mark()`方法. 如果返回值是 `true` 会触发 `HdfsBolt` 同步操作，之后再会调用 `reset()` 方法.

 `org.apache.storm.hdfs.sync.CountSyncPolicy` 会在发送指定数量的消息后，触发同步操作。

### 文件回滚策略
类似于同步策略，我们提供了文件回滚策略接口，允许你可以控制文件回滚时机。
`org.apache.storm.hdfs.rotation.FileRotation`:

```java
public interface FileRotationPolicy extends Serializable {
    boolean mark(Tuple tuple, long offset);
    void reset();
}
``` 

提供的`org.apache.storm.hdfs.rotation.FileSizeRotationPolicy` 实现，可以让你指定文件上限，一旦文件达到指定的大小之后，就会触发回滚操作。

```java
FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
```

### 文件回滚动作
你可以在HDFS bolt 和 Trident State注册任何回滚动作`RotationAction`s，一旦文件被触发了回滚，我们就会回调所有的回滚操作。比如，你可以将被
触发回滚的文件进行移动或者重命名

```java
public interface RotationAction extends Serializable {
    void execute(FileSystem fileSystem, Path filePath) throws IOException;
}
```

Storm-HDFS 提供了默认的回滚操作，就是对文件进行移动而已，具体参考如下：

```java
public class MoveFileAction implements RotationAction {
    private static final Logger LOG = LoggerFactory.getLogger(MoveFileAction.class);

    private String destination;

    public MoveFileAction withDestination(String destDir){
        destination = destDir;
        return this;
    }

    @Override
    public void execute(FileSystem fileSystem, Path filePath) throws IOException {
        Path destPath = new Path(destination, filePath.getName());
        LOG.info("Moving file {} to {}", filePath, destPath);
        boolean success = fileSystem.rename(filePath, destPath);
        return;
    }
}
```

如果你用的是 Trident 和 sequence files 的话，你可以这么做：

```java
        HdfsState.Options seqOpts = new HdfsState.SequenceFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withSequenceFormat(new DefaultSequenceFormat("key", "data"))
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310")
                .addRotationAction(new MoveFileAction().withDestination("/dest2/"));
```


## HDFS Bolt 支持 HDFS Sequence 文件

`org.apache.storm.hdfs.bolt.SequenceFileBolt` 支持你写 storm data 到  HDFS sequence 文件:

```java
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".seq")
                .withPath("/data/");

        // create sequence format instance.
        DefaultSequenceFormat format = new DefaultSequenceFormat("timestamp", "sentence");

        SequenceFileBolt bolt = new SequenceFileBolt()
                .withFsUrl("hdfs://localhost:54310")
                .withFileNameFormat(fileNameFormat)
                .withSequenceFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .withCompressionType(SequenceFile.CompressionType.RECORD)
                .withCompressionCodec("deflate");
```

 `SequenceFileBolt` 要求你实现 `org.apache.storm.hdfs.bolt.format.SequenceFormat`， 以便可以将Tuples映射成K/V 数据：
 
```java
public interface SequenceFormat extends Serializable {
    Class keyClass();
    Class valueClass();

    Writable key(Tuple tuple);
    Writable value(Tuple tuple);
}
```

## HDFS Bolt 支持 Avro 文件

`org.apache.storm.hdfs.bolt.AvroGenericRecordBolt` 支持你直接 Avro 对象写到 HDFS :
 
```java
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".avro")
                .withPath("/data/");

        // create sequence format instance.
        DefaultSequenceFormat format = new DefaultSequenceFormat("timestamp", "sentence");

        AvroGenericRecordBolt bolt = new AvroGenericRecordBolt()
                .withFsUrl("hdfs://localhost:54310")
                .withFileNameFormat(fileNameFormat)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
```


avro bolt 基于传递过来的schema来写文件的，如果你传递过来是两种形式的schema，那么他读生成两个独立的文件。每个文件的回滚策略都是可自定义的。
如果传递过来的schema特别多的话，我们建议你配置一下最大同时可打开的文件句柄数量，以防止文件的打开/关闭/创建操作次数过多。
为了使用这个bolt，你需要Kryo serializers， 就想这样：

`AvroGenericRecordBolt.addAvroKryoSerializations(conf);`


## HDFS Bolt 也支持Trident API
storm-hdfs also includes a Trident `state` implementation for writing data to HDFS, with an API that closely mirrors
that of the bolts.

 ```java
         Fields hdfsFields = new Fields("field1", "field2");

         FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                 .withPath("/trident")
                 .withPrefix("trident")
                 .withExtension(".txt");

         RecordFormat recordFormat = new DelimitedRecordFormat()
                 .withFields(hdfsFields);

         FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310");

         StateFactory factory = new HdfsStateFactory().withOptions(options);

         TridentState state = stream
                 .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());
 ```

 To use the sequence file `State` implementation, use the `HdfsState.SequenceFileOptions`:

 ```java
        HdfsState.Options seqOpts = new HdfsState.SequenceFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withSequenceFormat(new DefaultSequenceFormat("key", "data"))
                .withRotationPolicy(rotationPolicy)
                .withFsUrl("hdfs://localhost:54310")
                .addRotationAction(new MoveFileAction().toDestination("/dest2/"));
```

### Note
Whenever a batch is replayed by storm (due to failures), the trident state implementation automatically removes 
duplicates from the current data file by copying the data up to the last transaction to another file. Since this 
operation involves a lot of data copy, ensure that the data files are rotated at reasonable sizes with `FileSizeRotationPolicy` 
and at reasonable intervals with `TimedRotationPolicy` so that the recovery can complete within topology.message.timeout.secs.

Also note with `TimedRotationPolicy` the files are never rotated in the middle of a batch even if the timer ticks, 
but only when a batch completes so that complete batches can be efficiently recovered in case of failures.


##Working with Secure HDFS（待译）

If your topology is going to interact with secure HDFS, your bolts/states needs to be authenticated by NameNode. We 
currently have 2 options to support this:

### Using HDFS delegation tokens 
Your administrator can configure nimbus to automatically get delegation tokens on behalf of the topology submitter user.
The nimbus need to start with following configurations:

nimbus.autocredential.plugins.classes : ["org.apache.storm.hdfs.common.security.AutoHDFS"] 
nimbus.credential.renewers.classes : ["org.apache.storm.hdfs.common.security.AutoHDFS"] 
hdfs.keytab.file: "/path/to/keytab/on/nimbus" (This is the keytab of hdfs super user that can impersonate other users.)
hdfs.kerberos.principal: "superuser@EXAMPLE.com" 
nimbus.credential.renewers.freq.secs : 82800 (23 hours, hdfs tokens needs to be renewed every 24 hours so this value should be
less then 24 hours.)
topology.hdfs.uri:"hdfs://host:port" (This is an optional config, by default we will use value of "fs.defaultFS" property
specified in hadoop's core-site.xml)

Your topology configuration should have:
topology.auto-credentials :["org.apache.storm.hdfs.common.security.AutoHDFS"] 

If nimbus did not have the above configuration you need to add it and then restart it. Ensure the hadoop configuration 
files(core-site.xml and hdfs-site.xml) and the storm-hdfs jar with all the dependencies is present in nimbus's classpath. 
Nimbus will use the keytab and principal specified in the config to authenticate with Namenode. From then on for every
topology submission, nimbus will impersonate the topology submitter user and acquire delegation tokens on behalf of the
topology submitter user. If topology was started with topology.auto-credentials set to AutoHDFS, nimbus will push the
delegation tokens to all the workers for your topology and the hdfs bolt/state will authenticate with namenode using 
these tokens.

As nimbus is impersonating topology submitter user, you need to ensure the user specified in hdfs.kerberos.principal 
has permissions to acquire tokens on behalf of other users. To achieve this you need to follow configuration directions 
listed on this link
http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html

You can read about setting up secure HDFS here: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html.

### Using keytabs on all worker hosts
If you have distributed the keytab files for hdfs user on all potential worker hosts then you can use this method. You should specify a 
hdfs config key using the method HdfsBolt/State.withconfigKey("somekey") and the value map of this key should have following 2 properties:

hdfs.keytab.file: "/path/to/keytab/"
hdfs.kerberos.principal: "user@EXAMPLE.com"

On worker hosts the bolt/trident-state code will use the keytab file with principal provided in the config to authenticate with 
Namenode. This method is little dangerous as you need to ensure all workers have the keytab file at the same location and you need
to remember this as you bring up new hosts in the cluster.

---

# HDFS Spout（待译）

Hdfs spout is intended to allow feeding data into Storm from a HDFS directory. 
It will actively monitor the directory to consume any new files that appear in the directory.
HDFS spout does not support Trident currently.

**Impt**: Hdfs spout assumes that the files being made visible to it in the monitored directory 
are NOT actively being written to. Only after a file is completely written should it be made
visible to the spout. This can be achieved by either writing the files out to another directory 
and once completely written, move it to the monitored directory. Alternatively the file
can be created with a '.ignore' suffix in the monitored directory and after data is completely 
written, rename it without the suffix. File names with a '.ignore' suffix are ignored
by the spout.

When the spout is actively consuming a file, it renames the file with a '.inprogress' suffix.
After consuming all the contents in the file, the file will be moved to a configurable *done* 
directory and the '.inprogress' suffix will be dropped.

**Concurrency** If multiple spout instances are used in the topology, each instance will consume
a different file. Synchronization among spout instances is done using lock files created in a 
(by default) '.lock' subdirectory under the monitored directory. A file with the same name
as the file being consumed (without the in progress suffix) is created in the lock directory.
Once the file is completely consumed, the corresponding lock file is deleted.

**Recovery from failure**
Periodically, the spout also records progress information wrt to how much of the file has been
consumed in the lock file. In case of an crash of the spout instance (or force kill of topology) 
another spout can take over the file and resume from the location recorded in the lock file.

Certain error conditions (such spout crashing) can leave behind lock files without deleting them. 
Such a stale lock file also indicates that the corresponding input file has also not been completely 
processed. When detected, ownership of such stale lock files will be transferred to another spout.   
The configuration 'hdfsspout.lock.timeout.sec' is used to specify the duration of inactivity after 
which lock files should be considered stale. For lock file ownership transfer to succeed, the HDFS
lease on the file (from prev lock owner) should have expired. Spouts scan for stale lock files
before selecting the next file for consumption.

**Lock on *.lock* Directory**
Hdfs spout instances create a *DIRLOCK* file in the .lock directory to co-ordinate certain accesses to 
the .lock dir itself. A spout will try to create it when it needs access to the .lock directory and
then delete it when done.  In error conditions such as a topology crash, force kill or untimely death 
of a spout, this file may not get deleted. Future running instances of the spout will eventually recover
this once the DIRLOCK file becomes stale due to inactivity for hdfsspout.lock.timeout.sec seconds.

## Usage

The following example creates an HDFS spout that reads text files from HDFS path hdfs://localhost:54310/source.

```java
// Instantiate spout
HdfsSpout textReaderSpout = new HdfsSpout().withOutputFields(TextFileReader.defaultFields);
// HdfsSpout seqFileReaderSpout = new HdfsSpout().withOutputFields(SequenceFileReader.defaultFields);

// textReaderSpout.withConfigKey("custom.keyname"); // Optional. Not required normally unless you need to change the keyname use to provide hds settings. This keyname defaults to 'hdfs.config' 

// Configure it
Config conf = new Config();
conf.put(Configs.SOURCE_DIR, "hdfs://localhost:54310/source");
conf.put(Configs.ARCHIVE_DIR, "hdfs://localhost:54310/done");
conf.put(Configs.BAD_DIR, "hdfs://localhost:54310/badfiles");
conf.put(Configs.READER_TYPE, "text"); // or 'seq' for sequence files

// Create & configure topology
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("hdfsspout", textReaderSpout, SPOUT_NUM);

// Setup bolts and other topology configuration
     ..snip..

// Submit topology with config
StormSubmitter.submitTopologyWithProgressBar("topologyName", conf, builder.createTopology());
```

See sample HdfsSpoutTopolgy in storm-starter.

## Configuration Settings
Class HdfsSpout provided following methods for configuration:

`HdfsSpout withOutputFields(String... fields)` : This sets the names for the output fields. 
The number of fields depends upon the reader being used. For convenience, built-in reader types 
expose a static member called `defaultFields` that can be used for this. 
 
 `HdfsSpout withConfigKey(String configKey)`
Optional setting. It allows overriding the default key name ('hdfs.config') with new name for 
specifying HDFS configs. Typically used to specify kerberos keytab and principal.

**E.g:**
```java
    HashMap map = new HashMap();
    map.put("hdfs.keytab.file", "/path/to/keytab");
    map.put("hdfs.kerberos.principal","user@EXAMPLE.com");
    conf.set("hdfs.config", map)
```

Only settings mentioned in **bold** are required.

| Setting                      | Default     | Description |
|------------------------------|-------------|-------------|
|**hdfsspout.reader.type**     |             | Indicates the reader for the file format. Set to 'seq' for reading sequence files or 'text' for text files. Set to a fully qualified class name if using a custom type (that implements interface org.apache.storm.hdfs.spout.FileReader)|
|**hdfsspout.hdfs**            |             | HDFS URI. Example:  hdfs://namenodehost:8020
|**hdfsspout.source.dir**      |             | HDFS location from where to read.  E.g. /data/inputfiles  |
|**hdfsspout.archive.dir**     |             | After a file is processed completely it will be moved to this directory. E.g. /data/done|
|**hdfsspout.badfiles.dir**    |             | if there is an error parsing a file's contents, the file is moved to this location.  E.g. /data/badfiles  |
|hdfsspout.lock.dir            | '.lock' subdirectory under hdfsspout.source.dir | Dir in which lock files will be created. Concurrent HDFS spout instances synchronize using *lock* files. Before processing a file the spout instance creates a lock file in this directory with same name as input file and deletes this lock file after processing the file. Spouts also periodically makes a note of their progress (wrt reading the input file) in the lock file so that another spout instance can resume progress on the same file if the spout dies for any reason.|
|hdfsspout.ignore.suffix       |   .ignore   | File names with this suffix in the in the hdfsspout.source.dir location will not be processed|
|hdfsspout.commit.count        |    20000    | Record progress in the lock file after these many records are processed. If set to 0, this criterion will not be used. |
|hdfsspout.commit.sec          |    10       | Record progress in the lock file after these many seconds have elapsed. Must be greater than 0 |
|hdfsspout.max.outstanding     |   10000     | Limits the number of unACKed tuples by pausing tuple generation (if ACKers are used in the topology) |
|hdfsspout.lock.timeout.sec    |  5 minutes  | Duration of inactivity after which a lock file is considered to be abandoned and ready for another spout to take ownership |
|hdfsspout.clocks.insync       |    true     | Indicates whether clocks on the storm machines are in sync (using services like NTP). Used for detecting stale locks. |
|hdfs.config (unless changed)  |             | Set it to a Map of Key/value pairs indicating the HDFS settigns to be used. For example, keytab and principle could be set using this. See section **Using keytabs on all worker hosts** under HDFS bolt below.| 

---

