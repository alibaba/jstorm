package com.alibaba.jstorm.client;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.Config;

import com.alibaba.fastjson.JSON;
import com.alibaba.jstorm.utils.JStormUtils;

public class ConfigExtension {
	/**
	 * if this configure has been set, the spout or bolt will log all receive
	 * tuples
	 * 
	 * topology.debug just for logging all sent tuples
	 */
	protected static final String TOPOLOGY_DEBUG_RECV_TUPLE = "topology.debug.recv.tuple";

	public static void setTopologyDebugRecvTuple(Map conf, boolean debug) {
		conf.put(TOPOLOGY_DEBUG_RECV_TUPLE, Boolean.valueOf(debug));
	}

	public static Boolean isTopologyDebugRecvTuple(Map conf) {
		return JStormUtils.parseBoolean(conf.get(TOPOLOGY_DEBUG_RECV_TUPLE),
				false);
	}

	/**
	 * port number of deamon httpserver server
	 */
	private static final Integer DEFAULT_DEAMON_HTTPSERVER_PORT = 7621;

	protected static final String SUPERVISOR_DEAMON_HTTPSERVER_PORT = "supervisor.deamon.logview.port";

	public static Integer getSupervisorDeamonHttpserverPort(Map conf) {
		return JStormUtils.parseInt(
				conf.get(SUPERVISOR_DEAMON_HTTPSERVER_PORT),
				DEFAULT_DEAMON_HTTPSERVER_PORT + 1);
	}

	protected static final String NIMBUS_DEAMON_HTTPSERVER_PORT = "nimbus.deamon.logview.port";

	public static Integer getNimbusDeamonHttpserverPort(Map conf) {
		return JStormUtils.parseInt(conf.get(NIMBUS_DEAMON_HTTPSERVER_PORT),
				DEFAULT_DEAMON_HTTPSERVER_PORT);
	}

	/**
	 * Worker gc parameter
	 * 
	 * 
	 */
	protected static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";

	public static void setWorkerGc(Map conf, String gc) {
		conf.put(WORKER_GC_CHILDOPTS, gc);
	}

	public static String getWorkerGc(Map conf) {
		return (String) conf.get(WORKER_GC_CHILDOPTS);
	}

	protected static final String WOREKER_REDIRECT_OUTPUT = "worker.redirect.output";

	public static boolean getWorkerRedirectOutput(Map conf) {
		Object result = conf.get(WOREKER_REDIRECT_OUTPUT);
		if (result == null)
			return true;
		return (Boolean) result;
	}
	
	protected static final String WOREKER_REDIRECT_OUTPUT_FILE = "worker.redirect.output.file";
	
	public static void setWorkerRedirectOutputFile(Map conf, String outputPath) {
		conf.put(WOREKER_REDIRECT_OUTPUT_FILE, outputPath);
	}
	
	public static String getWorkerRedirectOutputFile(Map conf) {
		return (String)conf.get(WOREKER_REDIRECT_OUTPUT_FILE);
	}

	/**
	 * Usually, spout finish prepare before bolt, so spout need wait several
	 * seconds so that bolt finish preparation
	 * 
	 * By default, the setting is 30 seconds
	 */
	protected static final String SPOUT_DELAY_RUN = "spout.delay.run";

	public static void setSpoutDelayRunSeconds(Map conf, int delay) {
		conf.put(SPOUT_DELAY_RUN, Integer.valueOf(delay));
	}

	public static int getSpoutDelayRunSeconds(Map conf) {
		return JStormUtils.parseInt(conf.get(SPOUT_DELAY_RUN), 30);
	}

	/**
	 * Default ZMQ Pending queue size
	 */
	public static final int DEFAULT_ZMQ_MAX_QUEUE_MSG = 1000;

	/**
	 * One task will alloc how many memory slot, the default setting is 1
	 */
	protected static final String MEM_SLOTS_PER_TASK = "memory.slots.per.task";

	@Deprecated
	public static void setMemSlotPerTask(Map conf, int slotNum) {
		if (slotNum < 1) {
			throw new InvalidParameterException();
		}
		conf.put(MEM_SLOTS_PER_TASK, Integer.valueOf(slotNum));
	}

	/**
	 * One task will use cpu slot number, the default setting is 1
	 */
	protected static final String CPU_SLOTS_PER_TASK = "cpu.slots.per.task";

	@Deprecated
	public static void setCpuSlotsPerTask(Map conf, int slotNum) {
		if (slotNum < 1) {
			throw new InvalidParameterException();
		}
		conf.put(CPU_SLOTS_PER_TASK, Integer.valueOf(slotNum));
	}

	/**
	 * if the setting has been set, the component's task must run different node
	 * This is conflict with USE_SINGLE_NODE
	 */
	protected static final String TASK_ON_DIFFERENT_NODE = "task.on.differ.node";

	public static void setTaskOnDifferentNode(Map conf, boolean isIsolate) {
		conf.put(TASK_ON_DIFFERENT_NODE, Boolean.valueOf(isIsolate));
	}

	public static boolean isTaskOnDifferentNode(Map conf) {
		return JStormUtils
				.parseBoolean(conf.get(TASK_ON_DIFFERENT_NODE), false);
	}

	protected static final String SUPERVISOR_ENABLE_CGROUP = "supervisor.enable.cgroup";

	public static boolean isEnableCgroup(Map conf) {
		return JStormUtils.parseBoolean(conf.get(SUPERVISOR_ENABLE_CGROUP),
				false);
	}

	/**
	 * If component or topology configuration set "use.old.assignment", will try
	 * use old assignment firstly
	 */
	protected static final String USE_OLD_ASSIGNMENT = "use.old.assignment";

	public static void setUseOldAssignment(Map conf, boolean useOld) {
		conf.put(USE_OLD_ASSIGNMENT, Boolean.valueOf(useOld));
	}

	/**
	 * The supervisor's hostname
	 */
	protected static final String SUPERVISOR_HOSTNAME = "supervisor.hostname";
	public static final Object SUPERVISOR_HOSTNAME_SCHEMA = String.class;

	public static String getSupervisorHost(Map conf) {
		return (String) conf.get(SUPERVISOR_HOSTNAME);
	}

	protected static final String SUPERVISOR_USE_IP = "supervisor.use.ip";

	public static boolean isSupervisorUseIp(Map conf) {
		return JStormUtils.parseBoolean(conf.get(SUPERVISOR_USE_IP), false);
	}

	protected static final String NIMBUS_USE_IP = "nimbus.use.ip";

	public static boolean isNimbusUseIp(Map conf) {
		return JStormUtils.parseBoolean(conf.get(NIMBUS_USE_IP), false);
	}

	protected static final String TOPOLOGY_ENABLE_CLASSLOADER = "topology.enable.classloader";

	public static boolean isEnableTopologyClassLoader(Map conf) {
		return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_CLASSLOADER),
				false);
	}

	public static void setEnableTopologyClassLoader(Map conf, boolean enable) {
		conf.put(TOPOLOGY_ENABLE_CLASSLOADER, Boolean.valueOf(enable));
	}

	protected static final String CONTAINER_NIMBUS_HEARTBEAT = "container.nimbus.heartbeat";

	/**
	 * Get to know whether nimbus is run under Apsara/Yarn container
	 * 
	 * @param conf
	 * @return
	 */
	public static boolean isEnableContainerNimbus() {
		String path = System.getenv(CONTAINER_NIMBUS_HEARTBEAT);

		if (StringUtils.isBlank(path)) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Get Apsara/Yarn nimbus container's hearbeat dir
	 * 
	 * @param conf
	 * @return
	 */
	public static String getContainerNimbusHearbeat() {
		return System.getenv(CONTAINER_NIMBUS_HEARTBEAT);
	}

	protected static final String CONTAINER_SUPERVISOR_HEARTBEAT = "container.supervisor.heartbeat";

	/**
	 * Get to know whether supervisor is run under Apsara/Yarn supervisor
	 * container
	 * 
	 * @param conf
	 * @return
	 */
	public static boolean isEnableContainerSupervisor() {
		String path = System.getenv(CONTAINER_SUPERVISOR_HEARTBEAT);

		if (StringUtils.isBlank(path)) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Get Apsara/Yarn supervisor container's hearbeat dir
	 * 
	 * @param conf
	 * @return
	 */
	public static String getContainerSupervisorHearbeat() {
		return (String) System.getenv(CONTAINER_SUPERVISOR_HEARTBEAT);
	}

	protected static final String CONTAINER_HEARTBEAT_TIMEOUT_SECONDS = "container.heartbeat.timeout.seconds";

	public static int getContainerHeartbeatTimeoutSeconds(Map conf) {
		return JStormUtils.parseInt(
				conf.get(CONTAINER_HEARTBEAT_TIMEOUT_SECONDS), 240);
	}

	protected static final String CONTAINER_HEARTBEAT_FREQUENCE = "container.heartbeat.frequence";

	public static int getContainerHeartbeatFrequence(Map conf) {
		return JStormUtils
				.parseInt(conf.get(CONTAINER_HEARTBEAT_FREQUENCE), 10);
	}

	protected static final String JAVA_SANDBOX_ENABLE = "java.sandbox.enable";

	public static boolean isJavaSandBoxEnable(Map conf) {
		return JStormUtils.parseBoolean(conf.get(JAVA_SANDBOX_ENABLE), false);
	}

	protected static String SPOUT_SINGLE_THREAD = "spout.single.thread";

	public static boolean isSpoutSingleThread(Map conf) {
		return JStormUtils.parseBoolean(conf.get(SPOUT_SINGLE_THREAD), false);
	}

	public static void setSpoutSingleThread(Map conf, boolean enable) {
		conf.put(SPOUT_SINGLE_THREAD, enable);
	}

	protected static String WORKER_STOP_WITHOUT_SUPERVISOR = "worker.stop.without.supervisor";

	public static boolean isWorkerStopWithoutSupervisor(Map conf) {
		return JStormUtils.parseBoolean(
				conf.get(WORKER_STOP_WITHOUT_SUPERVISOR), false);
	}

	protected static String CGROUP_ROOT_DIR = "supervisor.cgroup.rootdir";

	public static String getCgroupRootDir(Map conf) {
		return (String) conf.get(CGROUP_ROOT_DIR);
	}

	protected static String NETTY_TRANSFER_ASYNC_AND_BATCH = "storm.messaging.netty.transfer.async.batch";

	public static boolean isNettyTransferAsyncBatch(Map conf) {
		return JStormUtils.parseBoolean(
				conf.get(NETTY_TRANSFER_ASYNC_AND_BATCH), true);
	}

	protected static final String USE_USERDEFINE_ASSIGNMENT = "use.userdefine.assignment";

	public static void setUserDefineAssignment(Map conf,
			List<WorkerAssignment> userDefines) {
		List<String> ret = new ArrayList<String>();
		for (WorkerAssignment worker : userDefines) {
			ret.add(JSON.toJSONString(worker));
		}
		conf.put(USE_USERDEFINE_ASSIGNMENT, ret);
	}

	protected static final String MEMSIZE_PER_WORKER = "worker.memory.size";

	public static void setMemSizePerWorker(Map conf, long memSize) {
		conf.put(MEMSIZE_PER_WORKER, memSize);
	}

	protected static final String CPU_SLOT_PER_WORKER = "worker.cpu.slot.num";

	public static void setCpuSlotNumPerWorker(Map conf, int slotNum) {
		conf.put(CPU_SLOT_PER_WORKER, slotNum);
	}

	protected static String TOPOLOGY_PERFORMANCE_METRICS = "topology.performance.metrics";

	public static boolean isEnablePerformanceMetrics(Map conf) {
		return JStormUtils.parseBoolean(conf.get(TOPOLOGY_PERFORMANCE_METRICS),
				true);
	}

	public static void setPerformanceMetrics(Map conf, boolean isEnable) {
		conf.put(TOPOLOGY_PERFORMANCE_METRICS, isEnable);
	}

	protected static String NETTY_BUFFER_THRESHOLD_SIZE = "storm.messaging.netty.buffer.threshold";

	public static long getNettyBufferThresholdSize(Map conf) {
		return JStormUtils.parseLong(conf.get(NETTY_BUFFER_THRESHOLD_SIZE),
				8 *JStormUtils.SIZE_1_M);
	}

	public static void setNettyBufferThresholdSize(Map conf, long size) {
		conf.put(NETTY_BUFFER_THRESHOLD_SIZE, size);
	}
	
	protected static String NETTY_MAX_SEND_PENDING = "storm.messaging.netty.max.pending";
	
	public static void setNettyMaxSendPending(Map conf, long pending) {
		conf.put(NETTY_MAX_SEND_PENDING, pending);
	}
	
	public static long getNettyMaxSendPending(Map conf) {
		return JStormUtils.parseLong(conf.get(NETTY_MAX_SEND_PENDING), 16);
	}

	protected static String DISRUPTOR_USE_SLEEP = "disruptor.use.sleep";
    
    public static boolean isDisruptorUseSleep(Map conf) {
    	return JStormUtils.parseBoolean(conf.get(DISRUPTOR_USE_SLEEP), true);
    }
    
    public static void setDisruptorUseSleep(Map conf, boolean useSleep) {
    	conf.put(DISRUPTOR_USE_SLEEP, useSleep);
    }
    
    public static boolean isTopologyContainAcker(Map conf) {
    	int num = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
    	if (num > 0) {
    		return true;
    	}else {
    		return false;
    	}
    }
    
    protected static String NETTY_SYNC_MODE = "storm.messaging.netty.sync.mode";
    
    public static boolean isNettySyncMode(Map conf) {
    	return JStormUtils.parseBoolean(conf.get(NETTY_SYNC_MODE), false);
    }
    
    public static void setNettySyncMode(Map conf, boolean sync) {
    	conf.put(NETTY_SYNC_MODE, sync);
    }
    
    protected static String NETTY_ASYNC_BLOCK = "storm.messaging.netty.async.block";
    public static boolean isNettyASyncBlock(Map conf) {
    	return JStormUtils.parseBoolean(conf.get(NETTY_ASYNC_BLOCK), true);
    }
    
    public static void setNettyASyncBlock(Map conf, boolean block) {
    	conf.put(NETTY_ASYNC_BLOCK, block);
    }
    
    protected static String ALIMONITOR_METRICS_POST = "topology.alimonitor.metrics.post";
    
    public static boolean isAlimonitorMetricsPost(Map conf) {
    	return JStormUtils.parseBoolean(conf.get(ALIMONITOR_METRICS_POST), true);
    }
    
    public static void setAlimonitorMetricsPost(Map conf, boolean post) {
    	conf.put(ALIMONITOR_METRICS_POST, post);
    }
	
	protected static String TASK_CLEANUP_TIMEOUT_SEC = "task.cleanup.timeout.sec";
    
    public static int getTaskCleanupTimeoutSec(Map conf) {
    	return JStormUtils.parseInt(conf.get(TASK_CLEANUP_TIMEOUT_SEC), 10);
    }
    
    public static void setTaskCleanupTimeoutSec(Map conf, int timeout) {
    	conf.put(TASK_CLEANUP_TIMEOUT_SEC, timeout);
    }
    
    
}
