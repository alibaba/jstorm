---
title: JStorm Architecture
layout: plain
top-nav-title: JStorm Architecture
top-nav-group: advance
top-nav-pos: 1
sub-nav-title: JStorm Architecture
sub-nav-group: advance
sub-nav-pos: 1
---
## Architecture
![jiagou]({{site.baseurl}}/img/advance/architecture/architecture.jpg)

From a design perspective, Jstorm is a typical scheduling system

In this system,

1. Nimbus as a central scheduler is responsible for code distribution task assignment
2. Supervisor as a deamon on each machine is responsible for workers' start and stop
3. Worker is the container of tasks
4. Task is the real executor of business logic
5. Zookeeper is the coordinator of the whole system


## Work flow
1. Client submit one topology to nimbus
2. Nimbus assign task to different worker, generally the workers will be equal distributed in Supervisors. The task assignment will be stored in Zookeeper
3. Supervisor watch the Zookeeper, get to know which worker should be started and who will be shutdown.
4. If one worker has been started, it will fetch task assignment from Zookeeper and start the task's job.