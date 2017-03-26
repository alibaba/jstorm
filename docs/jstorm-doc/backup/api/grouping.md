---
title: Grouping
layout: plain
top-nav-title: Grouping
top-nav-group: api
top-nav-pos: 1
---
1. fieldsGrouping  -- this is like "group by" in SQL, tuples with the same value of target field will be sent to the same task 
1. globalGrouping -- all tuples will be sent to the first task of the component.
1. shuffleGrouping -- all tuples will be shuffle sent to tasks of the component, 
1. localOrShuffleGrouping -- if there are tasks of target component in current worker, then send tuples to these tasks with shuffle method, otherwise it is same as shuffleGrouping 
1. localFirst -- there are 3 kinds of tasks of target component, the first tasks are in the same worker, the second tasks are in same node but not same worker, the last tasks are run on other nodes. if there are the first tasks, then do shuffle in the first tasks, otherwise do shuffle in the second tasks, if neither the first tasks nor the second tasks exist, do shuffle in the last tasks.
1. noneGrouping -- all tuple will be random sent to tasks of the component, it is similar as shuffleGrouping, but it can't guarantee tuples be sent equally between the tasks.
1. allGrouping -- tuples will be sent to tasks
1. directGrouping -- tuples will be sent the specified task
1. customGrouping -- tuples will be sent to the user defined task