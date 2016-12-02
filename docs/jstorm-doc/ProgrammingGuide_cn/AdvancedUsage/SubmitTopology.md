---
title:  "使用API提交Topology"
layout: plain_cn

#sub-nav-parent: AdvancedUsage
sub-nav-group: AdvancedUsage_cn
sub-nav-id: SubmitTopology_cn
sub-nav-pos: 8
sub-nav-title: API 提交任务
---

* This will be replaced by the TOC
{:toc}

使用JavaAPI提交程序的核心接口是

```
public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts, List<File> jarFiles)
```

name表示TopologyName  
stormConf是conf对象,里面需要包括defaults.yaml中的参数,zkRoot,zkServer的IP  
topology是要提交的topology对象  
jarFiles是依赖的jar文件的文件对象  


下面是一个例子:

```
    public static void submit(StromTopology topology, File jarFile, String zkRoot, List<String> zkIps) {
        Yaml yaml = new Yaml();
        Map<String, Object> allConfig = new HashMap<String, Object>();
        allConfig.putAll(parse(yaml, "/defaults.yaml"));
        allConfig.put(Config.STORM_ZOOKEEPER_ROOT, zkRoot);
        allConfig.put(Config.STORM_ZOOKEEPER_SERVERS, zkIps);
        List<File> topologyJars = new ArrayList<File>();
        topologyJars.add(jarFile);
        StormSubmitter.submitTopology(name, allConfig, topology, null, topologyJars);
    }

    public static Map<String, Object> parse(Yaml yaml, String file) {
        @SuppressWarnings("unchecked")
        Map<String, Object> conf = (Map<String, Object>) yaml.load(new InputStreamReader(getClass()
            .getResourceAsStream(file)));
        if (conf == null) {
            conf = new HashMap<String, Object>();
        }
        return conf;
    }
	
```

[default.yaml下载地址](http://gitlab.alibaba-inc.com/aloha/aloha/blob/b46e758666cd7cb0acddf2b035c209d2a1105edf/jstorm-server/src/main/resources/defaults.yaml)