package com.alibaba.jstorm.yarn.constants;

/**
 * Constants used in both Client and Application Master
 */
public class JOYConstants {

    /**
     * Common
     */
    public static final String BLANK = " ";
    public static final String EMPTY = "";
    public static final String BACKLASH = "/";
    public static final String JAVA = "/bin/java";
    public static final String JAVA_CP = "/bin/java -cp ";
    public static final String COMMA = ",";
    public static final String COLON = ":";
    public static final String DOT = ".";
    public static final String EQUAL = "=";
    public static final String ASTERISK = "*";
    public static final String NEW_LINE = "\r\n";
    public static final String RPC_ADDRESS_FILE = "RpcAddress";
    public static final String CLI_PREFIX = "--";
    public static final String SCRIPT_PATH = "ExecScript";
    // Hardcoded path to shell script in launch container's local env
    public static final String ExecShellStringPath = JOYConstants.SCRIPT_PATH + ".sh";
    public static final String ExecBatScripStringtPath = JOYConstants.SCRIPT_PATH
            + ".bat";
    public static final String JSTORM_SUPERVISOR_HEARTBEAT_PATH = "/data/supervisor/supervisor.heartbeat";
    public static final String JSTORM_NIMBUS_HEARTBEAT_PATH = "/data/nimbus/nimbus.heartbeat";

    /**
     * Environment key name pointing to the shell script's location
     */
    public static final String DISTRIBUTEDSHELLSCRIPTLOCATION = "DISTRIBUTEDSHELLSCRIPTLOCATION";

    public static final String APPMASTERJARSCRIPTLOCATION = "APPMASTERJARSCRIPTLOCATION";

    public static final String BINARYFILEDEPLOYPATH = "BINARYFILEDEPLOYPATH";

    public static final String INSTANCENAME = "INSTANCENAME";
    /**
     * Environment key name denoting the file timestamp for the shell script.
     * Used to validate the local resource.
     */
    public static final String DISTRIBUTEDSHELLSCRIPTTIMESTAMP = "DISTRIBUTEDSHELLSCRIPTTIMESTAMP";
    public static final String APPMASTERTIMESTAMP = "APPMASTERTIMESTAMP";

    /**
     * Environment key name denoting the file content length for the shell script.
     * Used to validate the local resource.
     */
    public static final String DISTRIBUTEDSHELLSCRIPTLEN = "DISTRIBUTEDSHELLSCRIPTLEN";
    public static final String APPMASTERLEN = "APPMASTERLEN";

    /**
     * Environment key name denoting the timeline domain ID.
     */
    public static final String DISTRIBUTEDSHELLTIMELINEDOMAIN = "DISTRIBUTEDSHELLTIMELINEDOMAIN";


    public static final String CONTAINER_SUPERVISOR_HEARTBEAT = "container.supervisor.heartbeat";
    public static final String CONTAINER_NIMBUS_HEARTBEAT = "container.nimbus.heartbeat";
    public static final int EXECUTOR_HEARTBEAT_TIMEOUT = 60 * 1000;
    public static final int HOST_LOCK_TIMEOUT = 60 * 1000;
    public static final int PORT_RANGE_MIN = 9000;
    public static final int PORT_RANGE_MAX = 15000;

    /**
     * Executor Constance
     */
    public static final String YARN_SITE_PATH = "/etc/hadoop/yarn-site.xml";
    public static final String YARN_NM_LOG = "yarn.nodemanager.log-dirs";
    public static final String YARN_NM_LOG_DIR = "/dev/nm-logs";
    public static final String YARN_REGISTRY = "YarnRegistry";
    public static final String EXECUTOR_CLASS = "com.alibaba.jstorm.yarn.container.Executor";
    public static final String NEED_UPGRADE = "needUpgrade";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String SUPERVISOR = "supervisor";
    public static final String NIMBUS = "nimbus";
    public static final Integer EXECUTOR_LOOP_TIME = 30 * 1000;

    /**
     * yarn client
     */
    public static final String shellCommandPath = "shellCommands";
    public static final String shellArgsPath = "shellArgs";
    public static final Short FS_PERMISSION = 0710;
    public static final String CLIENT = "Client";
    public static final String appMasterJarPath = "AppMaster.jar";
    // Hardcoded path to custom log_properties
    public static final String log4jPath = "log4j.properties";

    public static final String APP_TYPE = "jstormonyarn";
    public static final String APP_NAME = "JstormOnYarn";

    public static final String APP_HEARTBEAT_TIME = "appHeartBeatTime";
    public static final String MASTER_CLASS_NAME = "com.alibaba.jstorm.yarn.appmaster.JstormMaster";

    public static final int HEARTBEAT_TIME_INTERVAL = 20000;
    public static final int MONITOR_TIME_INTERVAL = 1000;
    public static final int MONITOR_TIMES = 45;
    public static final String CONF_NAME = "jstorm-yarn.xml";
    public static final String RM_ADDRESS_KEY = "yarn.resourcemanager.address";
    public static final String INSTANCE_DEPLOY_DIR_KEY = "jstorm.yarn.instance.deploy.dir";
    public static final String INSTANCE_NAME_KEY = "jstorm.yarn.instance.name";
    public static final String SUPERVISOR_MIN_PORT_KEY = "jstorm.yarn.supervisor.minport";
    public static final String SUPERVISOR_MAX_PORT_KEY = "jstorm.yarn.supervisor.maxport";
    public static final String HADOOP_REGISTRY_ZK_RETRY_INTERVAL_MS = "hadoop.registry.zk.retry.interval.ms";
    public static final String INSTANCE_DATA_DIR_KEY = "jstorm.yarn.instance.dataDir";
    public static final String JSTORM_YARN_USER = "jstorm.yarn.user";
    public static final String JSTORM_YARN_PASSWORD = "jstorm.yarn.password";
    public static final String JSTORM_YARN_OLD_PASSWORD = "jstorm.yarn.oldpassword";
    public static final String FS_DEFAULTFS_KEY = "fs.defaultFS";
    public static final String YARN_CONF_MODE = "programatically";
    public static final String CLIIENT_CLASS = "com.alibaba.jstorm.yarn.JstormOnYarn";

    public static final String START_JSTORM_SHELL = "start_jstorm.sh";
    public static final String linux_bash_command = "bash";
    public static final String windows_command = "cmd /c";
    public static final Integer EXIT_SUCCESS = 0;
    public static final Integer EXIT_FAIL = -1;
    public static final Integer EXIT_FAIL1 = 1;
    public static final Integer EXIT_FAIL2 = 2;

    /**
     * application master
     */
    public static final String NIMBUS_HOST = "nimbus.host";
    public static final String NIMBUS_CONTAINER = "nimbus.containerId";
    public static final String NIMBUS_LOCAL_DIR = "nimbus.localdir";
    public static final String DEFAULT_LOGVIEW_PORT = "8622";
    public static final String DEFAULT_NIMBUS_THRIFT_PORT = "8627";
    public static final Integer DEFAULT_SUPERVISOR_MEMORY = 4110;
    public static final Integer DEFAULT_SUPERVISOR_VCORES = 1;
    public static final Integer AM_RM_CLIENT_INTERVAL = 1000;
    public static final String HADOOP_HOME_KEY = "jstorm.yarn.hadoop.home";
    public static final String JAVA_HOME_KEY = "jstorm.yarn.java.home";
    public static final String PYTHON_HOME_KEY = "jstorm.yarn.python.home";
    public static final String INSTANCE_DEPLOY_DEST_KEY = "jstorm.yarn.instance.deploy.destination";
    public static final String USER = "user";
    public static final String NODE = "Node";
    public static final String RESOURCES = "Resources";
    public static final String STATE = "State";
    public static final String EXIT_STATE = "Exit Status";
    public static final String START = "start";
    public static final String END = "end";
    public static final Integer JOIN_THREAD_TIMEOUT = 10000;

    /**
     * port view
     */
    public static final String COMPONENTS = "/components";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String AM = "am";
    public static final String CTIME = "cTime";
    public static final String DEFAULT_CTIME = "0";
    public static final String PORT_LIST = "portList";
    public static final String CONTAINER = "container";
    public static final String PORT_RANGE = "9111-9999";
    public static final String HTTP = "http";
    public static final String HOST_PORT = "host/port";
    public static final String RPC = "rpc";
    public static final Integer SLEEP_INTERVAL = 1000;
    public static final Integer RETRY_TIMES = 45;
    public static final Double JSTORM_MEMORY_WEIGHT = 4096.0;
    public static final Double JSTORM_VCORE_WEIGHT = 1.2;

    /**
     * cli options
     */
    public static final String APP_NAME_KEY = "appname";
    public static final String PRIORITY = "priority";
    public static final String QUEUE = "queue";
    public static final String TIMEOUT = "timeout";
    public static final String MASTER_MEMORY = "master_memory";
    public static final String MASTER_VCORES = "master_vcores";
    public static final String JAR = "jar";
    public static final String LIB_JAR = "lib_jars";
    public static final String HOME_DIR = "home_dir";
    public static final String CONF_FILE = "conf_file";
    public static final String APP_ATTEMPT_ID = "app_attempt_id";
    public static final String RM_ADDRESS = "rm_addr";
    public static final String NN_ADDRESS = "nn_addr";
    public static final String HADOOP_CONF_DIR = "hadoop_conf_dir";
    public static final String INSTANCE_NAME = "instance_name";
    public static final String DEPLOY_PATH = "deploy_path";
    public static final String SHELL_SCRIPT = "shell_script";
    public static final String SHELL_COMMAND = "shell_command";
    public static final String SHELL_ARGS = "shell_args";
    public static final String SHELL_ENV = "shell_env";
    public static final String SHELL_CMD_PRIORITY = "shell_cmd_priority";
    public static final String SHELL_CMD_PRIORITY_DEFAULT_VALUE = "0";
    public static final String CONTAINER_MEMORY = "container_memory";
    public static final String CONTAINER_VCORES = "container_vcores";
    public static final String NUM_CONTAINERS = "num_containers";
    public static final String LOG_PROPERTIES = "log_properties";
    public static final String KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS = "keep_containers_across_application_attempts";
    public static final String ATTEMPT_FAILURES_VALIDITY_INTERVAL = "attempt_failures_validity_interval";
    public static final String DEBUG = "debug";
    public static final String DOMAIN = "domain";
    public static final String XMX = "-Xmx";
    public static final String MB = "m";
    public static final String VIEW_ACLS = "view_acls";
    public static final String MODIFY_ACLS = "modify_acls";
    public static final String CREATE = "create";
    public static final String HELP = "help";
    public static final String NODE_LABEL_EXPRESSION = "node_label_expression";
    public static final String APPMASTER_STDOUT = "/AppMaster.stdout";
    public static final String APPMASTER_STDERR = "/AppMaster.stderr";
    public static final String JAVA_CLASS_PATH = "java.class.path";
    public static final String CLASS_PATH = "CLASSPATH";
    public static final String DEFAULT_CONTAINER_MEMORY = "10000";
    public static final String DEFAULT_CONTAINER_VCORES = "1";
    public static final String DEFAULT_MASTER_MEMORY = "10000";
    public static final String DEFAULT_MASTER_VCORES = "1";
    public static final String DEFAULT_PRIORITY = "0";
    public static final String XML = "xml";
    public static final String QUEUE_NAME = "default";
    public static final String DEFAULT_NUM_CONTAINER = "1";
    public static final String DEFAULT_CLIENT_TIME_OUT = "600000";
    public static final String DEFAULT_ATTEMPT_FAILURES_VALIDITY_INTERVAL = "-1";
}