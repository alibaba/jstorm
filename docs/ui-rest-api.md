# JStorm UI REST API

The JStorm UI daemon provides a REST API that allows you to interact with a Storm cluster, which includes retrieving
metrics data and configuration information as well as management operations such as starting or stopping topologies.


# Data format

The REST API returns JSON responses and supports JSONP.
Clients can pass a callback query parameter to wrap JSON in the callback function.


# Using the UI REST API

_Note: It is recommended to ignore undocumented elements in the JSON response because future versions of Storm may not_
_support those elements anymore._


## REST API Base URL

The REST API is part of the UI daemon of Storm (started by `storm ui`) and thus runs on the same host and port as the
Storm UI (the UI daemon is often run on the same host as the Nimbus daemon).  The port is configured by `ui.port`,
which is set to `8080` by default (see [defaults.yaml](conf/defaults.yaml)).

The API base URL would thus be:

    http://<ui-host>:<ui-port>/api/v2/...

You can use a tool such as `curl` to talk to the REST API:

    # Request the cluster configuration.
    # Note: We assume ui.port is configured to the default value of 8080.
    $ curl http://<ui-host>:8080/api/v2/cluster/configuration

##Impersonating a user in secure environment
In a secure environment an authenticated user can impersonate another user. To impersonate a user the caller must pass
`doAsUser` param or header with value set to the user that the request needs to be performed as. Please see SECURITY.MD
to learn more about how to setup impersonation ACLs and authorization. The rest API uses the same configs and acls that
are used by nimbus.

Examples:

```no-highlight
 1. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/wordcount-1-1425844354\?doAsUser=testUSer1
 2. curl 'http://localhost:8080/api/v2/cluster/Storm/topology/wordcount-1-1425844354/activate' -X POST -H 'doAsUser:testUSer1'
```

## GET Operations

### /api/v2/clusters/summary (GET)

Returns all clusters summary information such as nimbus uptime or number of supervisors.

Response fields:

|Field  |Value|Description
|---	|---	|---
|clusterName|String| Cluster Name|
|stormVersion|String| Storm version|
|supervisors|Integer| Number of supervisors running|
|topologies| Integer| Number of topologies running|
|tasksTotal| Integer |Total tasks|
|slotsTotal| Integer|Total number of available worker slots|
|slotsUsed| Integer| Number of worker slots used|
|slotsFree| Integer |Number of worker slots available|


Sample response:

```json
[
	{
		"clusterName": "Cluster1",
		"stormVersion": "1.0.0",
		"supervisors": 7,
		"topologies": 3,
		"taskTotal": 32,
		"slotsTotal": 20,
		"slotsUsed": 6,
		"slotsFree": 14
	},
	{
		"clusterName": "Cluster2",
		"stormVersion": "1.0.0",
		"supervisors": 3,
		"topologies": 3,
		"taskTotal": 31,
		"slotsTotal": 12,
		"slotsUsed": 7,
		"slotsFree": 5
	}
]
```

### /api/v2/cluster/:cluster_name/configuration (GET)

Returns the cluster configuration. Substitute cluster_name with cluster name.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|


Sample response (does not include all the data fields):

```json
{
	"drpc.worker.threads": 64,
	"topology.state.synchronization.timeout.secs": 60,
	"topology.executor.send.buffer.size": 1024,
	"topology.max.spout.pending": null,
	"storm.auth.simple-acl.users": [],
	"task.cleanup.timeout.sec": 10,
	"task.batch.tuple": true,
	"drpc.authorizer.acl.filename": "drpc-auth-acl.yaml",
	"nimbus.classpath": "",
	"worker.redirect.output.file": null,
	"task.msg.batch.size": 6,
	"nimbus.credential.renewers.freq.secs": 600,
	"storm.zookeeper.auth.password": null,
	"supervisor.slots.ports": null,
	...
}
```

### /api/v2/cluster/:cluster_name/supervisor/:ip/configuration (GET)

Returns the cluster configuration. Substitute cluster_name with cluster name.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|
|ip   	   |String (required)| Supervisor's IP  |


Sample response (does not include all the data fields):

```json
{
	"worker.redirect.output.file": null,
	"task.msg.batch.size": 6,
	"nimbus.credential.renewers.freq.secs": 600,
	"storm.zookeeper.auth.password": null,
	"supervisor.slots.ports": [
		6800,
		6801
		],
	...
}
```

### /api/v2/cluster/:cluster_name/topology/:id/configuration (GET)

Returns the cluster configuration. Substitute cluster_name with cluster name.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|
|id   	   |String (required)| Topology Id  |


Sample response (does not include all the data fields):

```json
{
	"topology.id": "SequenceTest6-9-1445920116",
	"fall.back.on.java.serialization": true,
	"topology.acker.executors": 1,
	"topology.spout.parallel": 4,
	"topology.name": "SequenceTest6",
	"user.group": null,
	"worker.redirect.output.file": "/proc/mounts",
	...
}
```

### /api/v2/cluster/:cluster_name/summary (GET)

Returns cluster summary information such as nimbus uptime or number of supervisors.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|

Response fields:

|Field  |Value|Description
|---	|---	|---
|clusterName|String| Cluster Name|
|stormVersion|String| Storm version|
|supervisors|Integer| Number of supervisors running|
|topologies| Integer| Number of topologies running|
|tasksTotal| Integer |Total tasks|
|slotsTotal| Integer|Total number of available worker slots|
|slotsUsed| Integer| Number of worker slots used|
|slotsFree| Integer |Number of worker slots available|

Sample response:

```json
{
	"clusterName": "Cluster1",
	"stormVersion": "1.0.0",
	"supervisors": 7,
	"topologies": 3,
	"taskTotal": 32,
	"slotsTotal": 20,
	"slotsUsed": 6,
	"slotsFree": 14
}
```

### /api/v2/cluster/:cluster_name/metrics (GET)

Returns cluster level metrics information in latest 30 minutes such as CpuUsedRatio or MemoryUsed.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|

Response fields:

|Field  |Value|Description
|---	|---	|---
|name|String| Metric name|
|data|List| latest 30 minutes metric value|
|label|List| latest 30 minutes metric formatted value|
|category| List| latest 30 minutes time|

Sample response:

```json
{
	"metrics": [
		{
			"name": "CpuUsedRatio",
			"data": [390.00000,412.0999999999999,400.4,...,418.29999999999995],
			"label": ["390","412.1","400.4",...,"418.3"],
			"category": ["11-13 15:32","11-13 15:33","11-13 15:34",...,"11-13 16:01"]
		},
		{
			"name": "MemoryUsed",
			"data": [...],
			"label": [...],
			"category": [...]
		}
		...
	]
}
```

### /api/v2/cluster/:cluster_name/supervisor/summary (GET)

Returns summary information for all supervisors.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|

Response fields:

|Field  |Value|Description|
|---	|---	|---
|id| String | Supervisor's id|
|host| String| Supervisor's host name|
|ip| String| Supervisor's IP|
|uptime| String| Shows how long the supervisor is running|
|uptimeSeconds| Integer| Shows how long the supervisor is running in seconds|
|slotsTotal| Integer| Total number of available worker slots for this supervisor|
|slotsUsed| Integer| Number of worker slots used on this supervisor|

Sample response:

```json
{
	"supervisors": [
		{
			"id": "19c99637-3335-472f-8330-9489196b0842",
			"host": "v1.storm.tbsite.net",
			"ip": "10.125.5.216",
			"uptime": "15d 4h 0m 1s",
			"uptimeSeconds": 1310401,
			"slotsTotal": 2,
			"slotsUsed": 1
		}
    ]
}
```

### /api/v2/cluster/:cluster_name/nimbus/summary (GET)

Returns summary information for all nimbus hosts.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|

Response fields:

|Field  |Value|Description|
|---	|---	|---
|host| String | Nimbus' host name|
|ip| String | Nimbus'IP|
|port| int| Nimbus' port number|
|status| String| Possible values are `Master` or `Slave`|
|nimbusUpTime| String| Shows since how long the nimbus has been running|
|nimbusUpTimeSeconds| String| Shows since how long the nimbus has been running in seconds|
|version| String| Version of storm this nimbus host is running|
|

Sample response:

```json
{
	"nimbus": [
		{
			"host": "v1.storm.tbsite.net",
			"ip": "10.218.135.80",
			"port": "7627",
			"status": "Master",
			"nimbusUpTime": "10d 5h 51m 2s",
			"nimbusUpTimeSeconds": "885062",
			"supervisors": 7,
			"slotsTotal": 20,
			"slotsUsed": 6,
			"slotsFree": 14,
			"taskTotal": 32,
			"topologies": 3,
			"version": "1.0.0"
		},
		{
			"host": "v2.storm.tbsite.net",
			"ip": "10.218.132.134",
			"port": ":7627",
			"status": "Slave",
			"nimbusUpTime": "3m 33s",
			"nimbusUpTimeSeconds": "213",
			"supervisors": 0,
			"slotsTotal": 0,
			"slotsUsed": 0,
			"slotsFree": 0,
			"taskTotal": 0,
			"topologies": 0,
			"version": "1.0.0"
		}
	]
}
```

### /api/v2/cluster/:cluster_name/zookeeper/summary (GET)

Returns summary information for all zookeepers.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|

Response fields:

|Field  |Value|Description|
|---	|---	|---
|host| String| Zookeeper's host name|
|ip| String| Zookeeper's IP|
|port|String| Zookeeper's port|

Sample response:

```json
{
	"zookeepers": [
		{
			"host": "v4.storm.tbsite.net",
			"ip": "10.125.4.193",
			"port": "2181"
		}
	]
}
```

### /api/v2/cluster/:cluster_name/topology/summary (GET)

Returns summary information for all topologies.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|

Response fields:

|Field  |Value | Description|
|---	|---	|---
|id| String| Topology Id|
|name| String| Topology Name|
|status| String| Topology Status|
|uptime| String|  Shows how long the topology is running|
|uptimeSeconds| Integer|  Shows how long the topology is running in seconds|
|tasksTotal| Integer |Total number of tasks for this topology|
|workersTotal| Integer |Number of workers used for this topology|


Sample response:

```json
{
	"topologies": [
		{
			"id": "WordCount3-1-1402960825",
			"name": "WordCount3",
			"status": "ACTIVE",
			"uptime": "17d 10h 46m 39s",
			"uptimeSeconds": 1507599,
			"tasksTotal": 14,
			"workersTotal": 4
		}
	]
}
```

### /api/v2/cluster/:cluster_name/topology/:id/metrics (GET)

Returns topology level metrics information in latest 30 minutes such as CpuUsedRatio or MemoryUsed.

Request parameters:

|Parameter	| Value	| Description |
|---        |---    |---          |
|cluster_name| String(Required)| cluster name|
|id   	   |String (required)| Topology Id  |

Response fields:

|Field  |Value|Description
|---	|---	|---
|name|String| Metric name|
|data|List| latest 30 minutes metric value|
|label|List| latest 30 minutes metric formatted value|
|category| List| latest 30 minutes time|

Sample response:

```json
{
	"metrics": [
		{
			"name": "CpuUsedRatio",
			"data": [390.00000,412.0999999999999,400.4,...,418.29999999999995],
			"label": ["390","412.1","400.4",...,"418.3"],
			"category": ["11-13 15:32","11-13 15:33","11-13 15:34",...,"11-13 16:01"]
		},
		{
			"name": "MemoryUsed",
			"data": [...],
			"label": [...],
			"category": [...]
		}
		...
	]
}
```

### /api/v2/cluster/:cluster_name/topology/:id/graph (GET)

Returns topology graph struct. It is used to draw the topology graph.

Request parameters:

|Parameter |Value   |Description  |
|----------|--------|-------------|
|cluster_name| String(Required)| cluster name|
|id   	   |String (required)| Topology Id  |

Response fields:

|Field  |Value |Description|
|---	|---	|---
|depth| int|the graph depth|
|breadth| int |the graph breadth|
|nodes| List | the topology components|
|nodes.id | String | unique id for node,  generally is component name|
|nodes.value| long | the number of emitted from this component|
|nodes.label| String | component name|
|nodes.title|String| tooltip for the node|
|node.level|String| the node level in the hierarchical graph|
|nodes.hidden|boolean| whether to hide this node|
|nodes.spout|boolean| whether the node is spout|
|nodes.mapValue|Hash| some statistics for this node|
|edges|List| the data flow in components|
|edges.id|int|unique id for edge|
|edges.from|String|Edges are between two nodes, one to and one from. This is the from node id|
|edges.to|String|This is the to node id|
|edges.value|double|the stream tps between the two nodes|
|edges.title|String| tooltip for the edge|
|edges.cycleValue|double|the tuple life cycle time between the two nodes|
|edges.hidden|boolean| whether to hide this edge|

Sample response:

```json
{
	"graph": {
		"depth": 4,
		"breadth": 2
		"nodes": [
			{
				"id": "Customer",
				"value": 2477088,
				"label": "Customer",
				"title": "Emitted: 2,477,088",
				"level": 2,
				"mapValue": {
					"RecvTps": {
						"60": "20,661.63",
						"600": "18,428.48",
						"7200": "19,565.26",
						"86400": "20,286.75"
					},
					"Emitted": {
						"60": "2,477,088",
						"600": "24,256,568",
						"7200": "293,596,897",
						"86400": "3,537,779,014"
					},
					"SendTps": {
						"60": "41,219.2",
						"600": "18,419.91",
						"7200": "40,905.61",
						"86400": "41,904.83"
					}
				},
				"hidden": false,
				"spout": false
			},
			...
		],
		"edges": [
			{
				"id": 5,
				"from": "Trade",
				"to": "Merge",
				"value": 20660.670444991396,
				"title": "TPS: 20,660.67<br/>TupleLifeCycle: 49.91ms",
				"cycleValue": 49.90856031128418,
				"mapValue": {
					"TupleLifeCycle(ms)": {
							"60": "49.91",
							"600": "52.67",
							"7200": "49.07",
							"86400": "51.28"
						},
						"TPS": {
							"60": "20,660.67",
							"600": "9,215.58",
							"7200": "19,956.33",
							"86400": "20,310.74"
					}
				},
				"hidden": false
			},
			...
		]
	}
}
```



### /api/v2/cluster/:cluster_name/topology/:id (GET)

Returns topology information and statistics.  Substitute id with topology id.

Request parameters:

|Parameter |Value   |Description  |
|----------|--------|-------------|
|cluster_name| String(Required)| cluster name|
|id   	   |String (required)| Topology Id  |
|window    |String. Default value :60 seconds| Window duration for metrics in seconds|



Response fields:

|Field  |Value |Description|
|---	|---	|---
|id| String| Topology Id|
|name| String |Topology Name|
|uptime| String |How long the topology has been running|
|uptimeSeconds| Integer |How long the topology has been running in seconds|
|status| String |Current status of the topology, e.g. "ACTIVE"|
|tasksTotal| Integer |Total number of tasks for this topology|
|workersTotal| Integer |Number of workers used for this topology|
|windowHint|String|window param value in "ddhhmmss" format. Default value is "0d0h1m0s"|
|topologyMetrics|Hash|topology level statistics information|
|topologyMetrics.metrics|Hash|topology level metrics|
|componentMetrics|Array|component level statistics information|
|componentMetrics.metrics|Hash|component level metrics|
|componentMetrics.componentName|String|component name|
|componentMetrics.parallel|Integer|component parallelism hint|
|componentMetrics.type|String|component type, "spout" or "blot"|
|componentMetrics.errors|Array of Errors|List of component errors|
|componentMetrics.errors.errorTime|Integer|Timestamp when the exception occurred |
|componentMetrics.errors.errorLapsedSecs|Integer|Number of seconds elapsed since the error happened in a component|
|componentMetrics.errors.error|String|Shows the error happened in a component|
|userDefinedMetrics|Array|user defined metrics information|
|userDefinedMetrics.metricName|String|name of user defined metric|
|userDefinedMetrics.componentName|String|component name of user defined metric|
|userDefinedMetrics.type|String|type of  user defined metric, e.g. "Timer", "Meter"|
|userDefinedMetrics.value|String|value of user defined metric|
|workerMetrics|Array|worker level statistics information|
|workerMetrics.metrics|Hash|worker level metrics|
|workerMetrics.host|String|the worker host name|
|workerMetrics.port|String|the worker port number|
|taskStats|Array|Array of tasks statistics|
|taskStats.id|Integer|task id|
|taskStats.component|String|component name|
|taskStats.type|String|component type, "spout" or "bolt"|
|taskStats.uptime|String|How long the task has been running|
|taskStats.uptimeSeconds|Integer|How long the task has been running in seconds|
|taskStats.status|String|Current status of the task, e.g. "ACTIVE"|
|taskStats.host|String|the worker's ip on which the task is running |
|taskStats.port|String|the worker's port on which the task is running|
|taskStats.errors|Array of Errors|List of tasks errors|
|taskStats.errors.errorTime|Integer|Timestamp when the exception occurred |
|taskStats.errors.errorLapsedSecs|Integer|Number of seconds elapsed since the error happened in a task|
|taskStats.errors.error|String|Shows the error happened in a task|


Examples:

```no-highlight
 1. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/SequenceTest6-9-1445920116
 2. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/SequenceTest6-9-1445920116?window=7200
```

Sample response:

```json
 {
	"id": "SequenceTest6-9-1445920116",
	"name": "SequenceTest6",
	"uptime": "19d 1h 29m 52s",
	"uptimeSeconds": 1646992,
	"windowHint": "0d2h0m0s",
	"status": "ACTIVE",
	"workersTotal": 4,
	"tasksTotal": 14,
	"topologyMetrics": {
		"metrics": {
			"CpuUsedRatio": "181.4",
			"MemoryUsed": "2099000000",
			"RecvTps": "80,065.96",
			"Acked": "1,899,186,638",
			"Failed": "0",
			"Emitted": "1,899,116,227",
			"SendTps": "98,472.95",
			"ProcessLatency": "94,737.11"
		}
	},
	"componentMetrics": [
		{
			"metrics": {
					"AckerTime": "10.31",
					"EmitTime": "19.86",
					"SerializeTime": "5.87",
					"RecvTps": "20,301.82",
					"Acked": "146,136,567",
					"ExecutorTime": "78.16",
					"Failed": "0",
					"DeserializeTime": "6.12",
					"Emitted": "292,229,907",
					"SendTps": "38,203.62",
					"ProcessLatency": "94,133.25",
					"TupleLifeCycle": "4,486.03"
			},
			"componentName": "SequenceSpout",
			"parallel": 2,
			"type": "spout",
			"errors": null
		},
		...
	],
	"userDefinedMetrics": [
		{
			"metricName": "name4",
			"componentName": "Total",
			"type": "Timer",
			"value": "19,751.15"
		},
		{
			"metricName": "name2",
			"componentName": "Total",
			"type": "Timer",
			"value": "999,165,930,015,668"
		},
		...
	],
	"workerMetrics": [
		{
			"metrics": {
				"CpuUsedRatio": "146.2",
				"MemoryUsed": "1100000000",
				"NetworkMsgDecodeTime": "0.05",
				"DiskUsage": "0.41",
				"NettyServerRecvSpeed": "3,911,810.16",
				"NettyClientSendSpeed": "1,621,405.97"
			},
			"host": "10.125.66.180",
			"port": "6801"
		},
		...
	],
	"taskStats": [
		{
			"task_id": 1,
			"component": "Customer",
			"type": "bolt",
			"uptime": 1646970,
			"status": "ACTIVE",
			"host": "10.125.66.180",
			"port": 6801,
			"errors": [
				{
					"errorTime": 1447482291,
					"error": "Customer:1-ExecutorQueue is full",
					"errorLapsedSecs": 84818
				}
			]
		},
		...
	]
}
```


### /api/v2/cluster/:cluster_name/topology/:id/component/:component (GET)

Returns detailed metrics for the specific component including statistics and metrics of tasks belongs to the component.

Request parameters:

|Parameter |Value   |Description  |
|----------|--------|-------------|
|cluster_name| String(Required)| cluster name|
|id   	   |String (required)| Topology Id  |
|component |String (required)| Component Id |
|window    |String. Default value :60 seconds| Window duration for metrics in seconds|

Response fields:

|Field  |Value |Description|
|---	|---	|---
|topologyId   | String | Topology id|
|topologyName | String | Topology name|
|componentMetrics|Array|component level statistics information|
|componentMetrics.metrics|Hash|component level metrics|
|componentMetrics.componentName|String|component name|
|componentMetrics.parallel|Integer|component parallelism hint|
|componentMetrics.type|String|component type, "spout" or "blot"|
|componentMetrics.errors|Array of Errors|List of component errors|
|componentMetrics.errors.errorTime|Integer|Timestamp when the exception occurred |
|componentMetrics.errors.errorLapsedSecs|Integer|Number of seconds elapsed since the error happened in a component|
|taskMetrics|Array|Array of tasks metrics|
|taskMetrics.id|Integer| task id|
|taskMetrics.metrics|Hash|task metrics|
|taskStats|Array|Array of tasks statistics|
|taskStats.id|Integer|task id|
|taskStats.component|String|component name|
|taskStats.type|String|component type, "spout" or "bolt"|
|taskStats.uptime|String|How long the task has been running|
|taskStats.uptimeSeconds|Integer|How long the task has been running in seconds|
|taskStats.status|String|Current status of the task, e.g. "ACTIVE"|
|taskStats.host|String|the worker's ip on which the task is running |
|taskStats.port|String|the worker's port on which the task is running|
|taskStats.errors|Array of Errors|List of tasks errors|
|taskStats.errors.errorTime|Integer|Timestamp when the exception occurred |
|taskStats.errors.errorLapsedSecs|Integer|Number of seconds elapsed since the error happened in a task|
|taskStats.errors.error|String|Shows the error happened in a task|


Examples:

```no-highlight
1. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/SequenceTest6-9-1445920116/component/SequenceSpout
2. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/SequenceTest6-9-1445920116/component/SequenceSpout?window=7200
```

Sample response:

```json
{
	"topologyId": "SequenceTest6-9-1445920116",
	"topologyName": "SequenceTest6",
	"componentMetrics": {
		"metrics": {
				"AckerTime": "10.31",
				"EmitTime": "19.86",
				"SerializeTime": "5.87",
				"RecvTps": "20,301.82",
				"Acked": "146,136,567",
				"ExecutorTime": "78.16",
				"Failed": "0",
				"DeserializeTime": "6.12",
				"Emitted": "292,229,907",
				"SendTps": "38,203.62",
				"ProcessLatency": "94,133.25",
				"TupleLifeCycle": "4,486.03"
		},
		"componentName": "SequenceSpout",
		"parallel": 2,
		"type": "spout",
		"errors": [
				{
					"errorTime": 1447482291,
					"error": "SequenceSpout:1-ExecutorQueue is full",
					"errorLapsedSecs": 84818
				}
			]
	},
	"taskMetrics": [
		{
			"metrics": {
				"SerializeTime": "6.46",
				"Acked": "638,065",
				"DeserializeQueue": "0.00",
				"ExecutorTime": "177.3",
				"ProcessLatency": "23.11",
				"ExecutorQueue": "0.00",
				"EmitTime": "20.96",
				"SerializeQueue": "0.00",
				"RecvTps": "10,344.85",
				"Emitted": "1,276,255",
				"DeserializeTime": "5.49",
				"SendTps": "20,580.65",
				"TupleLifeCycle": "14,510.7"
			},
			"taskId": "1"
		},
		...
	],
	"taskStats": [
		{
			"task_id": 1,
			"component": "SequenceSpout",
			"type": "spout",
			"uptime": 1646970,
			"status": "ACTIVE",
			"host": "10.125.66.180",
			"port": 6801,
			"errors": [
				{
					"errorTime": 1447482291,
					"error": "SequenceSpout:1-ExecutorQueue is full",
					"errorLapsedSecs": 84818
				}
			]
		},
		...
	]
}
```

### /api/v2/cluster/:cluster_name/topology/:id/task/:task\_id (GET)

Returns detailed metrics for the specific component including statistics and metrics of tasks belongs to the component.

Request parameters:

|Parameter |Value   |Description  |
|----------|--------|-------------|
|cluster_name| String(Required)| cluster name|
|id   	   |String (required)| Topology Id  |
|task_id |Integer (required)| Task Id |
|window    |String. Default value :60 seconds| Window duration for metrics in seconds|

Response fields:

|Field  |Value |Description|
|---	|---	|---
|topologyId   | String | Topology id|
|topologyName | String | Topology name|
|component|String|Component name|
|taskMetrics|Array|Array of tasks metrics|
|taskMetric.id|Integer| Task id|
|taskMetric.metrics|Hash|Task metrics|
|task|Hash|Task statistics|
|task.id|Integer|Task id|
|task.component|String|component name|
|task.type|String|component type, "spout" or "bolt"|
|task.uptime|String|How long the task has been running|
|task.uptimeSeconds|Integer|How long the task has been running in seconds|
|task.status|String|Current status of the task, e.g. "ACTIVE"|
|task.host|String|the worker's ip on which the task is running |
|task.port|String|the worker's port on which the task is running|
|task.errors|Array of Errors|List of tasks errors|
|task.errors.errorTime|Integer|Timestamp when the exception occurred |
|task.errors.errorLapsedSecs|Integer|Number of seconds elapsed since the error happened in a task|
|task.errors.error|String|Shows the error happened in a task|
|streamMetrics|Array|Array of stream metrics|
|streamMetrics.steamId|String|Stream name|
|streamMetrics.metrics|Hash|Stream metrics|


Examples:

```no-highlight
1. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/SequenceTest6-9-1445920116/component/SequenceSpout
2. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/topology/SequenceTest6-9-1445920116/component/SequenceSpout?window=7200
```

Sample response:

```json
{
	"topologyId": "SequenceTest6-9-1445920116",
	"topologyName": "SequenceTest6",
	"component": "Customer",
	"task": {
		"id": 1,
		"component": "Customer",
		"type": "bolt",
		"uptime": "19d 4h 55m 40s",
		"uptimeSeconds": 1659340,
		"status": "ACTIVE",
		"host": "10.125.66.180",
		"port": 6801,
		"errors": [
			{
				"errorTime": 1447482291,
				"error": "Customer:1-ExecutorQueue is full",
				"errorLapsedSecs": 97186
			}
		]
	},
	"taskMetric": {
		"metrics": {
			"SerializeTime": "31.4",
			"Acked": "628,604",
			"ExecutorTime": "81.61",
			"DeserializeQueue": "0.00",
			"ProcessLatency": "19.23",
			"ExecutorQueue": "12.00",
			"EmitTime": "11.71",
			"SerializeQueue": "0.00",
			"RecvTps": "10,523.43",
			"Emitted": "1,257,262",
			"DeserializeTime": "19.85",
			"SendTps": "21,046.43",
			"TupleLifeCycle": "12,677.04"
		},
		"taskId": "1"
	},
	"streamMetrics": [
		{
			"metrics": {
				"RecvTps": "10,523.43",
				"Acked": "628,604",
				"ProcessLatency": "19.23",
				"TupleLifeCycle": "12,677.04"
			},
			"streamId": "customer_stream"
		},
		{
			"metrics": {
				"Emitted": "628,660",
				"SendTps": "10,523.47"
			},
			"streamId": "__ack_ack"
		},
		...
	]
}
```

### /api/v2/cluster/:cluster_name/supervisor/:host (GET)

Returns status and worker metrics for the supervisor.

Request parameters:

|Parameter |Value   |Description  |
|----------|--------|-------------|
|cluster_name| String(Required)| cluster name|
|host|String(Required)|Supervisor's host name|
|window    |String. Default value :60 seconds| Window duration for metrics in seconds|

Response fields:

|Field  |Value |Description|
|---	|---	|---
|supervisor|Hash| Supervisor status information|
|supervisor.id|String| Supervisor's Id|
|supervisor.host|String| Supervisor's host name|
|supervisor.ip|String |Supervisor's ip|
|supervisor.uptime|String|How long the supervisor has been running|
|supervisor.uptimeSeconds|Integer|How long the supervisor has been running in seconds|
|supervisor.slotsUsed|Integer| Number of worker slots used|
|supervisor.slotsTotal|Integer| Number of worker slots total|
|workers|Array| Array of workers' status|
|workers.topology|String| worker's  topology id|
|workers.port|Integer| worker's port number|
|workers.uptime|String|How long the worker has been running|
|workers.uptimeSeconds|Integer|How long the worker has been running in seconds|
|workers.tasks|Array|Array of tasks' status running on the worker|
|workers.tasks.id|Integer|task's id|
|workers.tasks.component|String|task's component name|
|workerMetrics|Array|worker level statistics information|
|workerMetrics.metrics|Hash|worker level metrics|
|workerMetrics.host|String|the worker host name|
|workerMetrics.port|String|the worker port number|

Examples:

```no-highlight
1. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/supervisor/10.125.66.180
2. http://ui-daemon-host-name:8080/api/v2/cluster/Storm/supervisor/10.125.66.180?window=7200
```

Sample response:

```json
{
	"supervisor": {
		"id": "d0218ca6-9771-4462-a77b-3444e974c5bb",
		"host": "10.125.66.180",
		"ip": "10.125.66.180",
		"uptime": "12d 22h 57m 3s",
		"uptimeSeconds": 1119423,
		"slotsTotal": 2,
		"slotsUsed": 1
	},
	"workers": [
		{
			"topology": "SequenceTest6-9-1445920116",
			"port": 6801,
			"uptime": "19d 7h 25m 15s",
			"uptimeSeconds": 1668315,
			"tasks": [
				{
					"id": 1,
					"component": "Customer"
				},
				{
					"id": 5,
					"component": "Split"
				},
				{
					"id": 7,
					"component": "__acker"
				},
				{
					"id": 13,
					"component": "SequenceSpout"
				}
			]
		}
	],
	"workerMetrics": [
		{
			"metrics": {
				"CpuUsedRatio": "147",
				"MemoryUsed": "1100000000",
				"NetworkMsgDecodeTime": "0.19",
				"DiskUsage": "0.41",
				"NettyServerRecvSpeed": "4,276,960.64",
				"NettyClientSendSpeed": "1,767,554.59"
			},
			"host": "10.125.66.180",
			"port": "6801"
		}
	]
}
```

## API errors

The API returns 500 HTTP status codes in case of any errors.

Sample response:

```json
{
  "error": "Internal Server Error",
  "errorMessage": "java.lang.NullPointerException\n\tat clojure.core$name.invoke(core.clj:1505)\n\tat backtype.storm.ui.core$component_page.invoke(core.clj:752)\n\tat backtype.storm.ui.core$fn__7766.invoke(core.clj:782)\n\tat compojure.core$make_route$fn__5755.invoke(core.clj:93)\n\tat compojure.core$if_route$fn__5743.invoke(core.clj:39)\n\tat compojure.core$if_method$fn__5736.invoke(core.clj:24)\n\tat compojure.core$routing$fn__5761.invoke(core.clj:106)\n\tat clojure.core$some.invoke(core.clj:2443)\n\tat compojure.core$routing.doInvoke(core.clj:106)\n\tat clojure.lang.RestFn.applyTo(RestFn.java:139)\n\tat clojure.core$apply.invoke(core.clj:619)\n\tat compojure.core$routes$fn__5765.invoke(core.clj:111)\n\tat ring.middleware.reload$wrap_reload$fn__6880.invoke(reload.clj:14)\n\tat backtype.storm.ui.core$catch_errors$fn__7800.invoke(core.clj:836)\n\tat ring.middleware.keyword_params$wrap_keyword_params$fn__6319.invoke(keyword_params.clj:27)\n\tat ring.middleware.nested_params$wrap_nested_params$fn__6358.invoke(nested_params.clj:65)\n\tat ring.middleware.params$wrap_params$fn__6291.invoke(params.clj:55)\n\tat ring.middleware.multipart_params$wrap_multipart_params$fn__6386.invoke(multipart_params.clj:103)\n\tat ring.middleware.flash$wrap_flash$fn__6675.invoke(flash.clj:14)\n\tat ring.middleware.session$wrap_session$fn__6664.invoke(session.clj:43)\n\tat ring.middleware.cookies$wrap_cookies$fn__6595.invoke(cookies.clj:160)\n\tat ring.adapter.jetty$proxy_handler$fn__6112.invoke(jetty.clj:16)\n\tat ring.adapter.jetty.proxy$org.mortbay.jetty.handler.AbstractHandler$0.handle(Unknown Source)\n\tat org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)\n\tat org.mortbay.jetty.Server.handle(Server.java:326)\n\tat org.mortbay.jetty.HttpConnection.handleRequest(HttpConnection.java:542)\n\tat org.mortbay.jetty.HttpConnection$RequestHandler.headerComplete(HttpConnection.java:928)\n\tat org.mortbay.jetty.HttpParser.parseNext(HttpParser.java:549)\n\tat org.mortbay.jetty.HttpParser.parseAvailable(HttpParser.java:212)\n\tat org.mortbay.jetty.HttpConnection.handle(HttpConnection.java:404)\n\tat org.mortbay.jetty.bio.SocketConnector$Connection.run(SocketConnector.java:228)\n\tat org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)\n"
}
```


