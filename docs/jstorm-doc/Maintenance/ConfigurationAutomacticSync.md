---
title:  "How to enable supervisor automatically sync configuration from Nimbus."
# Top-level navigation
top-nav-group: Maintenance
top-nav-pos: 2
top-nav-title: Automatically Sync Configuration 
---

* This will be replaced by the TOC
{:toc}

## Precondition

The version of jstorm is greater than or equal to 2.2.0

## Background 

Currently modifying the jstorm cluster configuration (storm.yaml), needs manually access to the gateway to copy storm.yaml, edit, and then use the batch scp to copy to the cluster and restart the whole cluster. If you need to modify multiple clusters at the same time, it is too inefficient and costly.

The global configuration pushing is mainly developed for the batching modification of cluster configuration.

## Implementation

### Standalone

Add ConfigUpdateHandler plugin that allows dynamic modification of storm.yaml configuration to nimbus in jstorm-core. The default DefaultConfigUpdateHandler do nothing, the same as there is no plugin.

Implementing a DiamondConfigUpdateHandler (external / jstorm-configserver module), the plug-in configuration updates through the diamond. When an update is detected, it will backup the current storm.yaml (reservations up to three backups which are storm.yaml.1, storm.yaml.2, storm.yaml.3), and the latest configuration will be written to the storm .yaml.

The supervisor has a configuration update thread (SupervisorRefershConfig), requesting the latest configuration from nimbus once about every half a minute, and compare with the local configuration. If they are not the same, then do the backup and cover the local configuration to update the configuration at cluster level dynamically.

Generally speaking, the configuration updates are pushed from the diamond to the nimbus, nimbus and then pull from the supervisor.

### Jstorm-on-yarn

In the case of yarn, the situation is a little bit complex. Because some configuration items of the supervisor in yarn is dynamically generated, such as storm.local.dir, jstorm.log.dir, etc. Then if the configuration of supervisor is  directly overwrote by the configuration of nimbus, there is a problem.

Thus, in the case of yarn, an new configuration item is added and the default value is as below:

```
jstorm.yarn.conf.blacklist: "jstorm.home, jstorm.log.dir, storm.local.dir, java.library.path"
```

This configuration item indicates NOT to overwrite the local configurations of supervisor by the configurations of nimbus.

On implementation, when SupervisorRefershConfig is in initialization, these configuration items will be saved, called retainedYarnConfig.

These configuration items are filtered at the following comparison. If the configuration is different, then **the configuration of nimbus is used and appending the retainedYarnConfig at the end**. An new configuration file then will be generated and cover local storm.yaml.

Meanwhile, in order to distinguish whether it is yarn environment, it needs the supervisor of yarn to add `-Djstorm-on-yarn = true` parameter at startup.

Note that the yarn configuration blacklist currently supports only simple k-v format. If you configure the value in complexity format as list or map, there is a problem.

## Operation

1. Trigger configuration update for a particular cluster / all clusters in the management and control platform

2. The management and control platform will pull the current configuration from the nimbus, allowing users editing online and pushing after that.

3. Every cluster will saved the current configuration file in diamond, with a dataId as the cluster.name in configuration file. In order to ensure that all environments can receive the configuration, koala will pushed the configuration to all the environments in diamond (central and units).

4. Nimbus receives the configuration, do check with the local one. If they are not the same, then update it.

5. Supervisor pull configuration from nimbus, check with the local configuration and confirm if it needs to update depending on if it is in the yarn environment.

## TODO

Currently after receiving the new configuration, it will actually invoke RefreshableComponent.refresh to dynamically update the configuration. But currently supports limited dynamic update configurations. They are mainly metrics or log related configuration. We will support more configuration items which can updated automatically in version 2.2.1.