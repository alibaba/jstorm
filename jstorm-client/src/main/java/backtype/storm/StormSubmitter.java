package backtype.storm;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.fastjson.JSON;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should
 * run your program with the "storm jar" command from the command-line, and then
 * use this class to submit your topologies.
 */
public class StormSubmitter {
	public static Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);

	private static Nimbus.Iface localNimbus = null;

	public static void setLocalNimbus(Nimbus.Iface localNimbusHandler) {
		StormSubmitter.localNimbus = localNimbusHandler;
	}

	/**
	 * Submits a topology to run on the cluster. A topology runs forever or
	 * until explicitly killed.
	 * 
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	public static void submitTopology(String name, Map stormConf,
			StormTopology topology) throws AlreadyAliveException,
			InvalidTopologyException, TopologyAssignException {
		submitTopology(name, stormConf, topology, null);
	}

	/**
	 * Submits a topology to run on the cluster. A topology runs forever or
	 * until explicitly killed.
	 * 
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @param options
	 *            to manipulate the starting of the topology
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	public static void submitTopology(String name, Map stormConf,
			StormTopology topology, SubmitOptions opts)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException {
		if (!Utils.isValidConf(stormConf)) {
			throw new IllegalArgumentException(
					"Storm conf is not valid. Must be json-serializable");
		}
		stormConf = new HashMap(stormConf);
		stormConf.putAll(Utils.readCommandLineOpts());
		Map conf = Utils.readStormConfig();
		conf.putAll(stormConf);
		putUserInfo(conf, stormConf);
		try {
			String serConf = JSON.toJSONString(stormConf);
			if (localNimbus != null) {
				LOG.info("Submitting topology " + name + " in local mode");
				localNimbus.submitTopology(name, null, serConf, topology);
			} else {
				NimbusClient client = NimbusClient.getConfiguredClient(conf);
				if (topologyNameExists(conf, name)) {
					throw new RuntimeException("Topology with name `" + name
							+ "` already exists on cluster");
				}
				submitJar(conf);
				try {
					LOG.info("Submitting topology " + name
							+ " in distributed mode with conf " + serConf);
					if (opts != null) {
						client.getClient().submitTopologyWithOpts(name, path,
								serConf, topology, opts);
					} else {
						// this is for backwards compatibility
						client.getClient().submitTopology(name, path, serConf,
								topology);
					}
				} catch (InvalidTopologyException e) {
					LOG.warn("Topology submission exception", e);
					throw e;
				} catch (AlreadyAliveException e) {
					LOG.warn("Topology already alive exception", e);
					throw e;
				} catch (TopologyAssignException e) {
					LOG.warn("Failed to assign ", e.get_msg());
					throw e;
				} finally {
					client.close();
				}
			}
			LOG.info("Finished submitting topology: " + name);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean topologyNameExists(Map conf, String name) {
		NimbusClient client = NimbusClient.getConfiguredClient(conf);
		try {
			ClusterSummary summary = client.getClient().getClusterInfo();
			for (TopologySummary s : summary.get_topologies()) {
				if (s.get_name().equals(name)) {
					return true;
				}
			}
			return false;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			client.close();
		}
	}

	private static String submittedJar = null;
	private static String path = null;

	private static void submitJar(Map conf) {
		if (submittedJar == null) {
			NimbusClient client = NimbusClient.getConfiguredClient(conf);
			try {
				LOG.info("Jar not uploaded to master yet. Submitting jar...");
				String localJar = System.getProperty("storm.jar");
				path = client.getClient().beginFileUpload();
				String[] pathCache = path.split("/");
				String uploadLocation = path + "/stormjar-"
						+ pathCache[pathCache.length - 1] + ".jar";
				submittedJar = submitJar(conf, localJar, uploadLocation, client);
				List<String> lib = (List<String>) conf
						.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
				Map<String, String> libPath = (Map<String, String>) conf
						.get(GenericOptionsParser.TOPOLOGY_LIB_PATH);
				if (lib != null) {
					for (String libName : lib) {
						String jarPath = path + "/" + libName;
						client.getClient().beginLibUpload(jarPath);
						submitJar(conf, libPath.get(libName), jarPath, client);
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				client.close();
			}
		} else {
			LOG.info("Jar already uploaded to master. Not submitting jar.");
		}
	}

	public static String submitJar(Map conf, String localJar,
			String uploadLocation, NimbusClient client) {
		if (localJar == null) {
			throw new RuntimeException(
					"Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
		}

		try {

			LOG.info("Uploading topology jar " + localJar
					+ " to assigned location: " + uploadLocation);
			int bufferSize = 512 * 1024;
			Object maxBufSizeObject = conf
					.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE);
			if (maxBufSizeObject != null) {
				bufferSize = Utils.getInt(maxBufSizeObject) / 2;
			}

			BufferFileInputStream is = new BufferFileInputStream(localJar,
					bufferSize);
			while (true) {
				byte[] toSubmit = is.read();
				if (toSubmit.length == 0)
					break;
				client.getClient().uploadChunk(uploadLocation,
						ByteBuffer.wrap(toSubmit));
			}
			client.getClient().finishFileUpload(uploadLocation);
			LOG.info("Successfully uploaded topology jar to assigned location: "
					+ uploadLocation);
			return uploadLocation;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			
		}
	}

	private static void putUserInfo(Map conf, Map stormConf) {
		stormConf.put("user.group", conf.get("user.group"));
		stormConf.put("user.name", conf.get("user.name"));
		stormConf.put("user.password", conf.get("user.password"));
	}
}