# An Overall Comparison between JStorm & Storm  metrics
| --- | Storm/stats | Storm/built-in metrics | JStorm metrics
| --------   | :----- | :-------  | :--------  |
|windows | 10m, 3h, 1d, all-time | 1m | 1m, 10m, 2h, 1d
|sampling | 5%, all metrics sampled | same as stats | 10%, counters not sampled, meters/histograms sampled
|metric-stream | executors/tasks -> ZK | executor -> metrics consumer -> external systems | worker -> topology master -> nimbus -> external systems
|metrics | key-ed metrics | stream/executor metrics, topology metrics are computed upon calling | pre-computed metrics of stream/task/component/topology/cluster/worker/netty/nimbus metrics
|metrics data | sampled counters, mean value for meters/histograms | same as stats | counters not sampled, m1/m5/m15/mean for meters, p50/p75/p90/p95/p98/p99/p999/min/max/mean for histograms
|update stragety | every time bucket (very long for 3h/1d windows) | every minute | every minute for all windows
|zk dependency | write metrics to zk | N/A | N/A


# JStorm Metrics Design

## our goal
by re-designing the metrics system, we want to:
1. see all metrics from stream up to cluster level, updated at least every minute.
2. see all-time metrics data from multiple windows, not just a static point (of latest metric values).
3. support common metric types and more accurate metrics data.
4. support topology history, through which we can see metrics data of dead topologies.
5. support task track: we want to know where each task is allocated after a topology starts, i.e., from what time the task is (dead and) re-assigned from host1:port1 to host2:port2, and so forth.
6. easy to add plugins to store metrics to external storage, which makes implementing a monitor system upon JStorm to be quite easy.
7. full support of user-defined metrics.
8. simplify trouble-shooting through metrics.

## Basic work flow
```seq
worker->worker: create JStormMetricsReporter
worker->worker: JStormMetrics.registerMetrics to local metrics registry
worker->nimbus: JStormMetricsReporter.registerMetrics: register metrics meta
nimbus->nimbus: save metrics meta to JStormMetricsCache
nimbus->external systems: store metrics meta to external systems
nimbus->worker: return registered meta: map&lt;metricId, metricName&gt;

worker->topology_master: JstormMetricsReporter send metrics data to TopologyMaster
topology_master->topology_master: aggregate/compute metrics
topology_master->nimbus: send metrics data to nimbus
nimbus->nimbus: save metrics data to JStormMetricsCache
nimbus->external systems: send metrics data if external MetricsUploader exists
```


## Concepts:
### metric types
we currently support following metric types:
`counter/gauge/meter/histogram/timer`

(I'm considering of removing timer because histogram is enough in most scenarios)

### metrics (aka. metaType in JStorm metrics)
We currently support following metrics: 
`stream/task/component/netty/worker/topology/cluster/nimbus`

For topology/cluster metrics, we can easily summarize/monitor the overall resource usage (mem, cpu, disk, etc.) of a topology/cluster.

### metric names
Metric names are structured, full-qualified names (like java package names), which is totally different from Storm. 

For stream/task/component/netty metrics, a metric name is composed with: 
`metaType@metricType@topologyId@componentId@taskId@streamId@metricGroup@metricName`

When a user registers a stream metric, our metrics system will automatically register task/component metrics accordingly and links these metrics.

e.g., when a user registers an `emitted` counter metric for stream: default, taskId: 1, component: spout1, topologyId: SeqTest-1-1, the generated stream metric name will be:
`3@1@SeqTest-1-1@spout1@1@default@sys@emitted`
where 3 is the enum value of MetaType.STREAM, and 1 is the enum value of MetricType.COUNTER.

Also, after calling `registerStreamMetric(...)`, the corresponding task/component metrics are automatically registered with names:
`3@1@SeqTest-1-1@spout1@1@@sys@emitted`
`3@1@SeqTest-1-1@spout1@@@sys@emitted`

Simple enough, we just set stream id to be empty for task metric name, and set stream id & task id to be empty for component metric name.

Things are similar if a user want to register a topology metric or cluster metric, and we provide interfaces to let user register stream/task metrics only, we do automatic registration internally.

For worker/topology/cluster/nimbus metrics, a metric name is composed with:
metaType@metricType@topologyId@host@port@metricGroup@metricName
(for cluster/nimbus, the topologyId is set to `__CLUSTER__`/`__NIMBUS__`).

This metric name scheme can easily be employed by external systems like HBase to store metrics data.

### metric id
Because FQN metric names are too long to store in external systems, we separate it into metric meta & metric data. Metric meta consists of a map of metric id and metric names, while metric data consists of metric id and actual metric values. Currently we use a random long of GUID least 64 bits as metric id.

metric id mechanism does employ complexity, but it saves space, and it's not mandatory.

## Important modules of JStorm metrics
### JStormMetrics
A static class which offers `registerMetrics` methods, like codahale metrics, all metrics are kept in the memory metric registry, which reside in the worker process.
This class is responsible for automatic metrics registration.

### JStormMetricsReporter
It's a worker-level instance which is responsible for registering metrics to nimbus and sending metrics data to topology master/nimbus.

Note that this instance can exist not only in topology workers, it can also exist in supervisor/nimbus, which enables a supervisor/nimbus to report its metrics.

### TopologyMaster
responsible for aggregating/computing metrics data of all workers within a topology (it's actually done in `TopologyMetricContext` class), after that, it sends the metrics data to nimbus via thrift calls, once a minute, based on the window config.

### TopologyMetricsRunnable
This class resides in nimbus server. It's responsible for two things:
1. generate metric ids.
2. upload metrics meta/data to external systems.

### JStormMetricsCache
We employ rocksdb as the storage engine for nimbus cache & metrics cache to improve efficiency.

Internally, all metrics data sent from workers will be stored in rocksdb, waiting for `MetricsUploader` to handle. If no user-defined `MetricsUploader` is used, `DefaultMetricsUploader` will be used, which simply does nothing but delete expired metrics data, otherwise metrics data may be sent to external systems.

Another reason to use rocksdb is that, if we keep all metrics data in nimbus memory, nimbus may suffer from severe GC stress.


## Miscellaneous
### user-defined metrics
we provide a `MetricClient`, which enables user-defined metrics.
Like `JStormMetrics.registerMetrics...` methods, once user calls `metricClient.registerGauge/Counter/Histogram`, he can leave everything else to the metrics system. Component even topology metrics are automatically registered & summarized & computed.

### accuracy of metrics data
1. In order to ensure the accuracy of histograms, we don't average percentile values sent from all workers, instead, we not only send histogram percentile values, but also send sampled data points, and do phase2 computation with the data points in topology master.

2. counters are not sampled, so metrics like `emitted`, `acked`, `failed` are exactly accurate.
                        
3. all time-related metrics are measured in us instead of ms to improve accuracy. Metrics like `nextTuple_time` in ms makes no sense  since they are usually 0ms.

### topology history & task events
topology/task event hooks are added to enable such events can be sent to external systems via `MetricsUploader` interface.

### building metric monitor systems
We've built a monitor system upon the `MetricsUploader` interface.
Also we provide a MySQL MetricsUploader plugin, and plan to provide a HBase plugin.


## JStorm metrics usage
 

## Metric config
Following are the metric config options and corresponding explanation.

#### topology.enable.metrics
global metric switch, only for test purpose, DO NOT set the value to false in production.

#### topology.metric.sample.rate
metric sample rate, default to 0.1(10%), note this is valid for histograms only since counters/gauges are not sampled,
and meters also have it's internal decaying mechanism, we don't sample them either. 

#### topology.enable.metric.debug
used with option `topology.debug.metric.names` option. If set, will print metrics in JStormMetricsReporter before uploading.

#### topology.debug.metric.names
debug metric names, separate by ','. e.g., 'SendTps, RecvTps'

#### topology.enabled.metric.names
used in `updateTopology` method, the value is a string separated by ',', the metrics will be enabled on the fly.

#### topology.disabled.metric.names
used in `updateTopology` method, the value is a string separated by ',', the metrics will be disabled on the fly.

#### nimbus.metric.uploader.class
used for nimbus nodes, the upload plugin class for metrics which must implement `MetricUploader` interface.

#### nimbus.metric.query.client.class
used for nimbus nodes, the metric query client class for metrics which must implement `MetricQueryClient` interface.
it's mainly used to sync metric meta between nimbus nodes.

#### topology.max.worker.num.for.netty.metrics
because netty metrics are registered per netty connection, for a large topology, there will be numerous netty metrics (thus
numerous metric data), for this reason, we have a hard limit for netty metrics: if worker num of a topology exceeds 200,
netty metrics will be completely disabled. Besides, users might want to change the value to a smaller one, by using this
config, you can set a value, say 5, when the worker num exceeds 5, netty metrics will be disabled. To completely disable
netty metrics, set this value to 1 in storm.yaml.


## Metric uploader & metric query client
As above config has shown, we have 2 options for metric uploading: `nimbus.metric.uploader.class` and `nimbus.metric.query.client.class`.
Metric uploader class is responsible for uploading all metrics to external storage. It's important to have an efficiency
and fast metric uploader as well as a solid external storage to store the metric data/meta.
Our primary recommendation would be using HBase, which supports quite large TPS in very low latency.
 
Another concern that metric uploader should be well handled is nimbus GC. For a large cluster, there might be quite a bulk
of metric objects within the nimbus, to ease the GC overhead of nimbus, our current strategy is to store the metrics in 
rocks db in nimbus first, and let metric uploader class get the metrics when necessary. So a tip for writing a GC-friendly
metric uploader is that, retrieve metrics only when necessary. e.g., you can set up a thread pool within the metric uploader,
let's say, of 10 threads, each of which is writing metrics to HBase. You may also have a queue for the thread pool, our 
recommendation comes that, DO NOT get metrics before the thread actually runs, which means, DO NOT do the following:

> 1. create a runnable.
> 2. get metric data and put it in the runnable.
> 3. put the runnable to queue and wait for execution.

Instead, do the following:

> 1. create a runnable.
> 2. get metric data key/index and put it in the runnable (no actual metric data should be retrieved).
> 3. put the runnable to queue and wait for execution.

Because the metric data is retrieved in a lazy-load fashion, it will greatly ease the GC overhead of nimbus nodes.


## Using MetricClient
A great feature of JStorm metrics is that we provide a `MetricClient`, so users can define custom metrics easily while
enjoying the full features of JStorm metrics.

It's quite easy to define a custom metric:

1. define metric client instance.
```
private MetricClient metricClient;
```

2. in your prepare method of your bolt (or open in spout):

```
metricClient = new MetricClient(context);

Gauge<Double> gauge = new Gauge<Double>() {
    private Random random = new Random();

    @Override
    public Double getValue() {
        return random.nextDouble();
    }

};
myGauge = metricClient.registerGauge("myGauge", gauge);
myCounter = metricClient.registerCounter("myCounter");
myMeter = metricClient.registerMeter("myMeter");
myHistogram = metricClient.registerHistogram("myHistogram");
```

And it's basically done! you can now call `myCounter.update` or `myMeter.update` methods in your code, we'll calculate
& report & upload the metrics for you!

For full API, please refer to `MetricClient` class.


## TODO
1. simplify metrics data sent to topology master/nimbus
2. metrics data sync between multiple nimbus servers when `DefaultMetricsUploader` is used to avoid metrics data loss on nimbus failure.
