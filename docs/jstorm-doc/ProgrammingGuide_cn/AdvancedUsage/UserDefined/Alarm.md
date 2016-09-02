---
title: "User Defined Alarm, report error to system"
layout: plain_cn

# Sub navigation
sub-nav-parent: UserDefined_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Alarm_cn
#sub-nav-pos: 2
sub-nav-title: 自定义报错
---

* This will be replaced by the TOC
{:toc}

# report 一个error
report 一个普通error， 这个时候当前worker 不会自杀时， 

```
public static void reportError(TopologyContext topologyContext, String errorMessge) {
		StormClusterState zkCluster = topologyContext.getZkCluster();
		String topologyId = topologyContext.getTopologyId();
		int taskId = topologyContext.getThisTaskId();
		
		try {
			zkCluster.report_task_error(topologyId, taskId, errorMessge);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.info("Failed to report Error");
		}
		
	}
```

# panic 一个error

当worker需要自杀时， report 一个严重error
```
collector.reportError(Throwable e);


当task是spout时， 这个collector来自 SpoutOutputCollector

void open(Map conf, TopologyContext context, SpoutOutputCollector collector);

当task是bolt时，这个collector来自OutputCollector

 void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
```

