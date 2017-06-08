#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/python

import os
import sys
import random
import subprocess as sub
import getopt
import time

try:
    # python 3
    from urllib.parse import quote_plus
except ImportError:
    # python 2
    from urllib import quote_plus

def identity(x):
    return x

def cygpath(x):
    command = ["cygpath", "-wp", x]
    p = sub.Popen(command,stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split("\n")
    return lines[0]

if sys.platform == "cygwin":
    normclasspath = cygpath
else:
    normclasspath = identity

CLIENT_CONF_FILE = ""
JSTORM_DIR = "/".join(os.path.realpath( __file__ ).split("/")[:-2])
JSTORM_CONF_DIR = os.getenv("JSTORM_CONF_DIR", JSTORM_DIR + "/conf" )
STORM_YAML = os.path.join(JSTORM_CONF_DIR, 'storm.yaml')
LOGBACK_CONF = JSTORM_CONF_DIR + "/jstorm.logback.xml"
CONFIG_OPTS = []
EXCLUDE_JARS = []
INCLUDE_JARS = []
STATUS = 0

STORM_YAML_CONFIG = {}
if os.path.exists(STORM_YAML):
    for line in open(STORM_YAML):
        if not line.startswith('#'):
            kv = line.strip().split(':')
            if len(kv) == 2:
                kv[1] = kv[1].strip()
                if kv[1] == 'null':
                    kv[1] = None
                elif kv[1].startswith('"') and kv[1].endswith('"'):
                    kv[1] = kv[1][1:len(kv[1])-1]
                STORM_YAML_CONFIG[kv[0].strip()] = kv[1]


def check_java():
    check_java_cmd = 'which java'
    ret = os.system(check_java_cmd)
    if ret != 0:
        print("Failed to find java, please add java to PATH")
        sys.exit(-1)

def filter_array(array):
    ret = []
    for item in array:
        temp = item.strip()
        if temp != "":
            ret.append(temp)
    return ret

def get_exclude_jars():
    global EXCLUDE_JARS
    return " -Dexclude.jars=" + (','.join(EXCLUDE_JARS))


def get_config_opts():
    global CONFIG_OPTS
    return "-Dstorm.options=" + ','.join(map(quote_plus,CONFIG_OPTS))

def get_client_log_opts():
    ret = (" -Dstorm.root.logger=INFO,stdout -Dlogback.configurationFile=" + JSTORM_DIR +
           "/conf/client_logback.xml -Dlog4j.configuration=File:" + JSTORM_DIR + 
           "/conf/client_log4j.properties")
    if CLIENT_CONF_FILE != "":
        ret += (" -Dstorm.conf.file=" + CLIENT_CONF_FILE)
    return ret

def get_server_log_opts(module):
    jstorm_log_dir = get_log_dir()

    key = module + '.deamon.logview.port'
    is_yarn = STORM_YAML_CONFIG.has_key('jstorm.on.yarn') and (STORM_YAML_CONFIG['jstorm.on.yarn'].lower() == 'true')
    if is_yarn and STORM_YAML_CONFIG.has_key(key):
        filename = module + '-' + STORM_YAML_CONFIG[key] + '.log'
    else:
        filename = module + '.log'
    gc_log_path = jstorm_log_dir + "/" + module + "-gc-" + str(int(time.time())) + ".log"
    ret = (" -Xloggc:%s -Dlogfile.name=%s -Dlogback.configurationFile=%s -Djstorm.log.dir=%s "
           %(gc_log_path, filename, LOGBACK_CONF, jstorm_log_dir))
    return ret

if not os.path.exists(JSTORM_DIR + "/RELEASE"):
    print "******************************************"
    print "The jstorm client can only be run from within a release. You appear to be trying to " \
          "run the client from a checkout of JStorm's source code."
    print "\nYou can download a JStorm release "
    print "******************************************"
    sys.exit(1)  

def get_jars_full(adir):
    files = os.listdir(adir)
    ret = []
    for f in files:
        if not f.endswith(".jar"):
            continue
        filter = False
        for exclude_jar in EXCLUDE_JARS:
            if f.find(exclude_jar) >= 0:
                filter = True
                break
        
        if filter:
            print "Don't add " + f + " to classpath"
        else:
            ret.append(adir + "/" + f)
    return ret

def get_classpath(extrajars):
    ret = []
    ret.extend(extrajars)
    ret.extend(get_jars_full(JSTORM_DIR))
    ret.extend(get_jars_full(JSTORM_DIR + "/lib"))
    ret.extend(INCLUDE_JARS)
    return normclasspath(":".join(ret))
    
def get_external_classpath(external):
    return map(lambda x:JSTORM_DIR + "/lib/ext/" + x + "/*", external.split(","))

def confvalue(name, extrapaths):
    command = [
        "java", "-client", "-Xms256m", "-Xmx256m", get_config_opts(), "-cp", get_classpath(extrapaths), "backtype.storm.command.config_value", name
    ]
    p = sub.Popen(command, stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split("\n")
    for line in lines:
        tokens = line.split(" ")
        if tokens[0] == "VALUE:":
            return " ".join(tokens[1:])
    print "Failed to get config " + name
    print errors
    print output

def print_localconfvalue(name):
    """Syntax: [jstorm localconfvalue conf-name]

    Prints out the value for conf-name in the local JStorm configs.
    The local JStorm configs are the ones in ~/.jstorm/storm.yaml merged
    in with the configs in defaults.yaml.
    """
    print name + ": " + confvalue(name, [JSTORM_CONF_DIR])

def get_log_dir():
    cppaths = [JSTORM_CONF_DIR]
    jstorm_log_dir = confvalue("jstorm.log.dir", cppaths)
    if not jstorm_log_dir == "null":
       if not os.path.exists(jstorm_log_dir):
           os.makedirs(jstorm_log_dir)
    else:
       jstorm_log_dir = JSTORM_DIR + "/logs"
       if not os.path.exists(jstorm_log_dir):
           os.makedirs(jstorm_log_dir)
    return jstorm_log_dir

def print_remoteconfvalue(name):
    """Syntax: [jstorm remoteconfvalue conf-name]

    Prints out the value for conf-name in the cluster's JStorm configs.
    The cluster's JStorm configs are the ones in $STORM-PATH/conf/storm.yaml
    merged in with the configs in defaults.yaml.

    This command must be run on a cluster machine.
    """
    print name + ": " + confvalue(name, [JSTORM_CONF_DIR])


def exec_storm_class(klass, jvmtype="-client -Xms256m -Xmx256m", childopts="", extrajars=[], args=[]):
    nativepath = confvalue("java.library.path", extrajars)
    #args_str = " ".join(map(lambda s: "\"" + s + "\"", args))
    args_str = " ".join(args)
    print args_str
    command = "java " + jvmtype + " -Djstorm.home=" + JSTORM_DIR + " " + get_config_opts() + " -Djava.library.path=" \
              + nativepath + " " + childopts + " -cp " + get_classpath(extrajars) + " " + klass + " " + args_str
    print "Running: " + command
    global STATUS
    STATUS = os.execvp("java", filter_array(command.split(" ")))
    #STATUS = os.system(command)

def jar(jarfile, klass, *args):
    """Syntax: [jstorm jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments.
    The jstorm jars and configs in $JSTORM_CONF_DIR/storm.yaml are put on the classpath.
    The process is configured so that StormSubmitter
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    childopts = "-Dstorm.jar=" + jarfile + get_client_log_opts() + get_exclude_jars()
    exec_storm_class(
        klass,
        extrajars=[jarfile, JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        args=args,
        childopts=childopts)

def zktool(*args):
    """Syntax: [jstorm jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments.
    The jstorm jars and configs in ~/.jstorm are put on the classpath.
    The process is configured so that StormSubmitter
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "com.alibaba.jstorm.zk.ZkTool",
        extrajars=[ JSTORM_CONF_DIR, CLIENT_CONF_FILE],
        args=args,
        childopts=childopts)

def kill(*args):
    """Syntax: [jstorm kill topology-name [wait-time-secs]]

    Kills the topology with the name topology-name. JStorm will
    first deactivate the topology's spouts for the duration of
    the topology's message timeout to allow all messages currently
    being processed to finish processing. JStorm will then shutdown
    the workers and clean up their state. You can override the length
    of time JStorm waits between deactivation and shutdown.
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.kill_topology",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def activate(*args):
    """Syntax: [jstorm activate topology-name]

    Activates the specified topology's spouts.
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.activate",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def deactivate(*args):
    """Syntax: [jstorm deactivate topology-name]

    Deactivates the specified topology's spouts.
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.deactivate",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def rebalance(*args):
    """Syntax: [jstorm rebalance topology-name [-w wait-time-secs]]

    Sometimes you may wish to spread out where the workers for a topology
    are running. For example, let's say you have a 10 node cluster running
    4 workers per node, and then let's say you add another 10 nodes to
    the cluster. You may wish to have JStorm spread out the workers for the
    running topology so that each node runs 2 workers. One way to do this
    is to kill the topology and resubmit it, but JStorm provides a "rebalance"
    command that provides an easier way to do this.

    Rebalance will first deactivate the topology for the duration of the
    message timeout  and then redistribute
    the workers evenly around the cluster. The topology will then return to
    its previous state of activation (so a deactivated topology will still
    be deactivated and an activated topology will go back to being activated).
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.rebalance",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def restart(*args):
    """Syntax: [jstorm restart topology-name [conf]]
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.restart",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def update_topology(*args):
    """Syntax: [jstorm update_topology topology-name -jar [jarpath] -conf [confpath]]
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.update_topology",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def gray_upgrade(*args):
    """Syntax: [jstorm gray_upgrade topology-name -jar [jarpath] -conf [confpath] -worker [workerNum]
        -tpTtl [upgradeTtl] -workerTtl [workerTtl]]
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.gray_upgrade",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def rollback(*args):
    """Syntax: [jstorm rollback topology-name]"""
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.rollback_topology",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def complete_upgrade(*args):
    """Syntax: [jstorm complete_upgrade topology-name]"""
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.complete_upgrade",
        args=args,
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def nimbus():
    """Syntax: [jstorm nimbus]

    Launches the nimbus daemon. This command should be run under
    supervision with a tool like daemontools or monit.

    See Setting up a JStorm cluster for more information.
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    """
    cppaths = [JSTORM_CONF_DIR]
    nimbus_classpath = confvalue("nimbus.classpath", cppaths)
    nimbus_external_cp = get_external_classpath(confvalue("nimbus.external", cppaths))
    childopts = confvalue("nimbus.childopts", cppaths) + get_server_log_opts("nimbus")
    exec_storm_class(
        "com.alibaba.jstorm.daemon.nimbus.NimbusServer",
        jvmtype="-server",
        extrajars=(cppaths+nimbus_external_cp+[nimbus_classpath]),
        childopts=childopts)

def supervisor():
    """Syntax: [jstorm supervisor]

    Launches the supervisor daemon. This command should be run
    under supervision with a tool like daemontools or monit.

    See Setting up a JStorm cluster for more information.
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    """
    cppaths = [JSTORM_CONF_DIR]
    supervisor_classpath = get_external_classpath(confvalue("supervisor.external", cppaths))
    childopts = confvalue("supervisor.childopts", cppaths) + get_server_log_opts("supervisor")
    exec_storm_class(
        "com.alibaba.jstorm.daemon.supervisor.Supervisor",
        jvmtype="-server",
        extrajars=(cppaths+supervisor_classpath),
        childopts=childopts)

def drpc():
    """Syntax: [jstorm drpc]

    Launches a DRPC daemon. This command should be run under supervision 
    with a tool like daemontools or monit. 

    See Distributed RPC for more information.
    (https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
    """
    cppaths = [JSTORM_CONF_DIR]
    childopts = confvalue("drpc.childopts", cppaths) + get_server_log_opts("drpc")
    exec_storm_class(
        "com.alibaba.jstorm.drpc.Drpc", 
        jvmtype="-server", 
        extrajars=cppaths, 
        childopts=childopts)

def print_classpath():
    """Syntax: [jstorm classpath]

    Prints the classpath used by the jstorm client when running commands.
    """
    print get_classpath([])

def print_commands():
    """Print all client commands and link to documentation"""
    print "jstorm command [--config client_storm.yaml] [--exclude-jars exclude1.jar,exclude2.jar] [-c key1=value1,key2=value2][command parameter]"
    print "Commands:\n\t",  "\n\t".join(sorted(COMMANDS.keys()))
    print "\n\t[--config client_storm.yaml]\t\t\t optional, setting client's storm.yaml"
    print "\n\t[--exclude-jars exclude1.jar,exclude2.jar]\t optional, exclude jars, avoid jar conflict"
    print "\n\t[-c key1=value1 -c key2=value2 -c key3=[\\\"a\\\", \\\"b\\\"] -c key4={\\\"a\\\":\\\"a\\\",\\\"b\\\":\\\"b\\\"}]\t\t\t optional, add key=value pair to configuration"
    print "\nHelp:", "\n\thelp", "\n\thelp <command>"
    print "\nDocumentation for the jstorm client can be found at https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation\n"

def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if COMMANDS.has_key(command):
            print (COMMANDS[command].__doc__ or 
                  "No documentation provided for <%s>" % command)
        else:
           print "<%s> is not a valid command" % command
    else:
        print_commands()

def unknown_command(*args):
    print "Unknown command: [jstorm %s]" % ' '.join(sys.argv[1:])
    print_usage()

def metrics_monitor(*args):
    """Syntax: [jstorm metricsMonitor topologyname bool]
    Enable or disable the metrics monitor of one topology.
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.metrics_monitor", 
        args=args, 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def list(*args):
    """Syntax: [jstorm list]

    List cluster information
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.list", 
        args=args, 
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def blobstore(*args):
    """Syntax: [jstorm blobstore -m]

    migrate stormdist to blobstore
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.blobstore",
        args=args,
        jvmtype="-client -Xms256m -Xmx256m",
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

def blacklist(*args):
    """Syntax: [jstorm blacklist add|remove hostname]

        hostname which in blacklist won't scheduler by nimbus
    """
    childopts = get_client_log_opts()
    exec_storm_class(
        "backtype.storm.command.blacklist",
        args=args,
        jvmtype="-client -Xms256m -Xmx256m",
        extrajars=[JSTORM_CONF_DIR, JSTORM_DIR + "/bin", CLIENT_CONF_FILE],
        childopts=childopts)

COMMANDS = {"jar": jar, "kill": kill, "nimbus": nimbus, "zktool": zktool,
            "drpc": drpc, "supervisor": supervisor, "localconfvalue": print_localconfvalue,
            "remoteconfvalue": print_remoteconfvalue, "classpath": print_classpath,
            "activate": activate, "deactivate": deactivate, "rebalance": rebalance, "help": print_usage,
            "metrics_monitor": metrics_monitor, "list": list, "restart": restart, "update_topology": update_topology,
            "blobstore": blobstore, "blacklist": blacklist, "gray_upgrade": gray_upgrade, "rollback": rollback,
            "complete_upgrade": complete_upgrade}

def parse_config(config_list):
    global CONFIG_OPTS
    if len(config_list) > 0:
        for config in config_list:
            CONFIG_OPTS.append(config)

def parse_exclude_jars(jars):
    global EXCLUDE_JARS
    EXCLUDE_JARS = jars.split(",")
    print " Excludes jars:"
    print EXCLUDE_JARS
    
def parse_include_jars(jars):
    global INCLUDE_JARS
    INCLUDE_JARS = jars.split(",")
    print " Include jars:"
    print INCLUDE_JARS

def parse_config_opts(args):
  curr = args[:]
  curr.reverse()
  config_list = []
  args_list = []
  
  while len(curr) > 0:
    token = curr.pop()
    if token == "-c":
      config_list.append(curr.pop())
    elif token == "--config":
      global CLIENT_CONF_FILE
      CLIENT_CONF_FILE = curr.pop()
    elif token == "--exclude-jars":
      parse_exclude_jars(curr.pop())
    elif token == "--include-jars":
      parse_include_jars(curr.pop())
    else:
      args_list.append(token)
  
  return config_list, args_list
    
def main():
    if len(sys.argv) <= 1:
        print_usage()
        sys.exit(-1)
    global CONFIG_OPTS
    config_list, args = parse_config_opts(sys.argv[1:])
    parse_config(config_list)
    COMMAND = args[0]
    ARGS = args[1:]
    if COMMANDS.get(COMMAND) == None:
        unknown_command(COMMAND)
        sys.exit(-1)
    if len(ARGS) != 0 and ARGS[0] == "help":
        print_usage(COMMAND)
        sys.exit(0)
    try:
        (COMMANDS.get(COMMAND, "help"))(*ARGS)
    except Exception, msg:
        print(msg)
        print_usage(COMMAND)
        sys.exit(-1)
    sys.exit(STATUS)

if __name__ == "__main__":
    check_java()
    main()
