---
title:  "Build JStorm Source Code"
# Top-level navigation
top-nav-group: QuickStart
top-nav-pos: 6
top-nav-title: Compile JStorm
---

* This will be replaced by the TOC
{:toc}


# Build JStorm
jstorm is easy to compile and package.
```
mvn clean
mvn package assembly:assembly -Dmaven.test.skip=true
```
If you get into any compile err, feel free to create an issue or pull request.
# Submit PATCH to the JStorm community

 - fork jstorm to your own branch
 - create a new branch in your branch

```
$ git checkout master
$ git fetch origin
$ git merge origin/master
$ git checkout -b <local_test_branch>  # e.g. git checkout -b STORM-1234
```

 - modify the code
 - commit the code to your branch
 - create a pull request, click "Pull Request" button on your github page