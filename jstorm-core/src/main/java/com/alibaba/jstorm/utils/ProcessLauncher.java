package com.alibaba.jstorm.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.jstorm.cluster.StormConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;

import backtype.storm.utils.Utils;


/**
 * Launch one process for 3 purpose
 * 1. make the worker process become daemon process, release Supervisor's pressure
 * 2. catch the exception and log the error message to worker's log, when fail to launch worker, for example wrong jvm parameter 
 * 3. create the log dir, avoid fail to create worker's gc log when dir doesn't exist
 *
 * @author longda
 *
 */
public class ProcessLauncher {

	/**
	 * Due to ProcessLaucher will catch the exception when fail to start worker
	 * ProcessLauncher will use the same log as the worker's log
	 * So ProcessLauncher don't log until child process is over.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ProcessLauncher.class);


	private static class LogWriter extends Thread {
		private Logger logger;
		private BufferedReader in;

		public LogWriter(InputStream in, Logger logger) {
			this.in = new BufferedReader(new InputStreamReader(in));
			this.logger = logger;
		}

		public void run() {
			Logger logger = this.logger;
			BufferedReader in = this.in;
			String line;
			try {
				while ((line = in.readLine()) != null) {
					logger.info(line);
				}
			} catch (IOException e) {
				logger.error("Internal ERROR", e);
			} finally {
				try {
					in.close();
				} catch (IOException e) {
					logger.error("Internal ERROR", e);
				}
			}
		}


		public void close() throws Exception {
			this.join();
		}
	}

	private static class LauncherThread extends Thread {

		private String[] args;
		private int ret = 0;

		public LauncherThread(String[] args) {
			this.args = args;
		}

		public void run() {
			ProcessBuilder pb = new ProcessBuilder(args);
			Process p;
			try {
				p = pb.start();
			} catch (IOException e1) {
				// TODO Auto-generated catch block, log can't print here
				ret = -1;
				System.out.println("Failed to start " + JStormUtils.mk_list(args) + "\n" + e1);
				return;
			}

			try {
				ret = p.waitFor();
				LOG.info(JStormUtils.getOutput(p.getErrorStream()));
				LOG.info(JStormUtils.getOutput(p.getInputStream()));
				LOG.info("!!!! Wokrer shutdown quickly !!!!");
			} catch (InterruptedException e) {
				ret = 0;
				System.out.println("Successfully start process");
			} catch (Throwable e) {
				//ret = -1;
				LOG.error("Unknow exception" + e.getCause(), e);
			} finally {
				System.out.println("Begin to exit:" + ret);
				//JStormUtils.haltProcess(launcher.getResult());
				System.exit(ret);
			}
		}

		public int getResult() {
			return ret;
		}
	}

	public static int getSleepSeconds() {
		Map<Object, Object> conf = null;
		try {
			conf = Utils.readStormConfig();
		} catch (Exception e) {
			conf = new HashMap<Object, Object>();
		}
		return ConfigExtension.getProcessLauncherSleepSeconds(conf);
	}

	public static void main(String[] args) throws Exception{
		try {
			System.out.println("Enviroment:" + System.getenv());
			System.out.println("Properties:" + System.getProperties());

			int sleepSeconds = getSleepSeconds();
			int ret = -1;

			try {
				if (System.getenv("REDIRECT") != null && System.getenv("REDIRECT").equals("true")) {
					ProcessBuilder pb = new ProcessBuilder(args);
					Process p = pb.start();
					LogWriter err = null;
					LogWriter in = null;

					try {
						err = new LogWriter(p.getErrorStream(), LOG);
						err.start();
						in = new LogWriter(p.getInputStream(), LOG);
						in.start();
						ret = p.waitFor();
					} finally {
						if (err != null) err.close();
						if (in != null) in.close();
					}
				} else {
					//when worker is dead, supervisor can kill ProcessLauncher right now
					//once worker start, worker will kill the processLauncher
					String workerId = System.getenv("jstorm.workerId");
					if (StringUtils.isNotBlank(workerId)) {
						Map conf = Utils.readStormConfig();
						StormConfig.validate_distributed_mode(conf);
						String pidDir = StormConfig.worker_pids_root(conf, workerId);
						JStormServerUtils.createPid(pidDir);
					}

					LauncherThread launcher = new LauncherThread(args);
					launcher.start();
					Thread.sleep(sleepSeconds * 1000);
					launcher.interrupt();
					ret = launcher.getResult();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block

			} finally {
				System.out.println("Begin to exit:" + ret);
				//JStormUtils.haltProcess(launcher.getResult());
				JStormUtils.haltProcess(ret);
			}
		} catch (Exception e) {
			LOG.error("find error!!", e);
			throw e;
		}
	}
}
