package com.aerospike.newrelic.connector;

import static com.aerospike.newrelic.utils.Constants.CLUSTER_FALLBACK_NAME;
import static com.aerospike.newrelic.utils.Constants.DEFAULT_PLUGIN_NAME;
import static com.aerospike.newrelic.utils.Constants.LATENCY;
import static com.aerospike.newrelic.utils.Constants.LATENCY_BUCKETS;
import static com.aerospike.newrelic.utils.Constants.LATENCY_CATEGORY;
import static com.aerospike.newrelic.utils.Constants.LATENCY_STATS;
import static com.aerospike.newrelic.utils.Constants.METRIC_BASE_NAME;
import static com.aerospike.newrelic.utils.Constants.NAMESPACE_STATS;
import static com.aerospike.newrelic.utils.Constants.NODE_STATS;
import static com.aerospike.newrelic.utils.Constants.READS;
import static com.aerospike.newrelic.utils.Constants.SLASH;
import static com.aerospike.newrelic.utils.Constants.SUMMARY;
import static com.aerospike.newrelic.utils.Constants.THROUGHPUT_STATS;
import static com.aerospike.newrelic.utils.Constants.WRITES;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.newrelic.utils.Utils;
import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.configuration.ConfigurationException;
import com.newrelic.metrics.publish.util.Logger;

/**
 * Agent for Aerospike. This agent will log Aerospike statistics, namespace
 * statistics, latency and throughput metrics for an Aerospike node.
 * 
 * @author Aniruddha Atre
 */
public class AerospikeAgent extends Agent {

	private static final String GUID = "com.aerospike.newrelic.connector";
	private static final String VERSION = "1.0.0";

	private String user;
	private String password;
	private String host;
	private Integer port;
	private String clusterName;
	private Base base;
	private String metricBaseName;

	Map<String, Float> totalReadTps;
	Map<String, Float> totalWriteTps;
	Map<String, Map<String, Float>> clusterWideLatency;

	private static final Logger logger = Logger.getLogger(AerospikeAgent.class);

	/**
	 * Constructor for Aerospike Agent
	 * 
	 * @param Aerospike
	 *            node details (host, port, user, password, node name)
	 * @throws ConfigurationException
	 *             if error reading configuration parameters.
	 */
	public AerospikeAgent(String host, String port, String user, String password, String clusterName)
			throws ConfigurationException {
		super(GUID, VERSION);
		try {

			this.host = host;
			this.port = Integer.parseInt(port);
			this.user = user;
			this.password = password;
			this.clusterName = clusterName;
			this.metricBaseName = METRIC_BASE_NAME + SLASH + clusterName;

			/* creating map for cluster-wide reads and writes */
			totalReadTps = new HashMap<String, Float>();
			totalWriteTps = new HashMap<String, Float>();

			/* creating map for cluster-wide latency */
			clusterWideLatency = new HashMap<String, Map<String, Float>>();

			/* Creating AerospikeClient */
			this.base = new Base();
			this.base.createAerospikeClient(this.host, this.port, this.user, this.password);

			/* Set default values to readTpsHistory and writeTpsHistory */
			setDefaultsToTpsHistory();

			logger.info("Aerospike Agent initialized: ", formatAgentParams(host, port, user, password, clusterName));

		} catch (Exception exception) {
			logger.error("Error reading configuration parameters : ", exception);
			throw new ConfigurationException("Error reading configuration parameters...", exception);
		}
	}

	/**
	 * Format Agent parameters for logging
	 * 
	 * @param clusterName
	 * @param password
	 * @param user
	 * @param port
	 * @param host
	 * 
	 * @return A formatted String representing the Agent parameters
	 */
	private String formatAgentParams(String host, String port, String user, String password, String clusterName) {
		StringBuilder builder = new StringBuilder();
		builder.append("host: ").append(host).append(" | ");
		builder.append("port: ").append(port).append(" | ");
		builder.append("user: ").append(user == null ? "n/a" : user).append(" | ");
		builder.append("password: ").append(password == null ? "n/a" : password).append(" | ");
		builder.append("clusterName: ").append(clusterName);
		return builder.toString();
	}

	/**
	 * Method to set default values to readTpsHistory and writeTpsHistory
	 * 
	 */
	public void setDefaultsToTpsHistory() {
		Integer timeStamp = (int) Calendar.getInstance().get(Calendar.MILLISECOND);
		Node[] nodes = base.getAerospikeNodes(host);

		for (Node node : nodes) {
			Map<String, String> readTpsHistory = new HashMap<String, String>();
			readTpsHistory.put("x", Integer.toString(timeStamp));
			readTpsHistory.put("y", null);
			readTpsHistory.put("secondary", null);

			Main.readTpsHistory.put(node.getHost().name, readTpsHistory);

			Map<String, String> writeTpsHistory = new HashMap<String, String>();
			writeTpsHistory.put("x", Integer.toString(timeStamp));
			writeTpsHistory.put("y", null);
			writeTpsHistory.put("secondary", null);

			Main.readTpsHistory.put(node.getHost().name, writeTpsHistory);
		}
	}

	/**
	 * Method to return agent name.
	 * 
	 * @return String Aerospike Agent Name
	 */
	@Override
	public String getAgentName() {
		return clusterName.equals(CLUSTER_FALLBACK_NAME) ? DEFAULT_PLUGIN_NAME : clusterName;
	}

	/**
	 * Method to fetch node statistics from base and report to new relic.
	 * 
	 * @return Map<String, String> A map of node statistics
	 */
	public Map<String, Map<String, String>> reportNodeStatistics() {
		String nodeStatPrefix = metricBaseName + SLASH;
		Map<String, Map<String, String>> perNodeStats = new HashMap<String, Map<String, String>>();

		Float totalUsedMemory = (float) 0;
		Float totalMemory = (float) 0;

		Float totalUsedDisk = (float) 0;
		Float totalDisk = (float) 0;

		Float batchInitiate = (float) 0;

		/* set default values for cluster-wide TPS */
		initTps();

		Node[] nodes = base.getAerospikeNodes(this.host);
		for (Node node : nodes) {
			Map<String, String> nodeStats = base.getNodeStatistics(node);

			if (nodeStats != null) {
				totalUsedMemory += Float.parseFloat(nodeStats.get("used-bytes-memory"));
				totalMemory += Float.parseFloat(nodeStats.get("total-bytes-memory"));

				totalUsedDisk += Float.parseFloat(nodeStats.get("used-bytes-disk"));
				totalDisk += Float.parseFloat(nodeStats.get("total-bytes-disk"));

				batchInitiate += Float.parseFloat(nodeStats.get("batch_initiate"));

				for (Map.Entry<String, String> nodeStat : nodeStats.entrySet()) {
					reportMetric(nodeStatPrefix + node.getHost().name + SLASH + NODE_STATS + SLASH + nodeStat.getKey(),
							"", Float.parseFloat(nodeStat.getValue()));
				}
			}

			Map<String, String> memoryStats = base.getMemoryStats(nodeStats);
			Map<String, String> diskStats = base.getDiskStats(nodeStats);

			if (Utils.isValidNumber(diskStats.get("free-bytes-disk")))
				reportMetric(nodeStatPrefix + node.getHost().name + SLASH + NODE_STATS + "/disk_usage_free", "",
						Float.parseFloat(diskStats.get("free-bytes-disk")));

			if (Utils.isValidNumber(diskStats.get("total-bytes-disk")))
				reportMetric(nodeStatPrefix + node.getHost().name + SLASH + NODE_STATS + "/disk_usage_total", "",
						Float.parseFloat(diskStats.get("total-bytes-disk")));

			if (Utils.isValidNumber(memoryStats.get("free-bytes-memory")))
				reportMetric(nodeStatPrefix + node.getHost().name + SLASH + NODE_STATS + "/memory_usage_free", "",
						Float.parseFloat(memoryStats.get("free-bytes-memory")));

			if (Utils.isValidNumber(memoryStats.get("total-bytes-memory")))
				reportMetric(nodeStatPrefix + node.getHost().name + SLASH + NODE_STATS + "/memory_usage_total", "",
						Float.parseFloat(memoryStats.get("total-bytes-memory")));

			perNodeStats.put(node.getHost().name, nodeStats);
			/* Report the throughput metrics */
			reportThroughput(nodeStats, node);
		}

		if (nodes.length > 0)
			batchInitiate = batchInitiate / nodes.length;
		/* report summary metric */
		reportSummaryMetric(totalUsedMemory, totalMemory, totalUsedDisk, totalDisk, batchInitiate);
		/* report cluster-wide reads and writes */
		reportTotalTps();
		return perNodeStats;
	}

	/**
	 * Method to set default values in totalReadTps and totalWriteTps maps
	 * 
	 */
	public void initTps() {
		totalReadTps.clear();
		totalReadTps.put("y", (float) 0);
		totalReadTps.put("secondary", (float) 0);

		totalWriteTps.clear();
		totalWriteTps.put("y", (float) 0);
		totalWriteTps.put("secondary", (float) 0);
	}

	/**
	 * Method to report node throughput.
	 * 
	 * @param nodeStats
	 *            Map of node statistics
	 */
	public void reportThroughput(Map<String, String> nodeStats, Node node) {

		Map<String, Map<String, String>> tps = base.getThroughput(nodeStats, node);
		Map<String, String> readTps = tps.get("reads");
		Map<String, String> writeTps = tps.get("writes");

		String baseThroughputMetric = metricBaseName + SLASH;
		if (readTps != null) {
			if (readTps.get("y") == null) {
				totalReadTps.put("y", (float) 0);
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + READS
						+ SLASH + "success", "", 0);
			} else {
				totalReadTps.put("y", totalReadTps.get("y") + Integer.valueOf(readTps.get("y")));
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + READS
						+ SLASH + "success", "", Integer.valueOf(readTps.get("y")));
			}
			if (readTps.get("secondary") == null) {
				totalReadTps.put("secondary", (float) 0);
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + READS
						+ SLASH + "total", "", 0);
			} else {
				totalReadTps.put("secondary",
						totalReadTps.get("secondary") + Integer.valueOf(readTps.get("secondary")));
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + READS
						+ SLASH + "total", "", Integer.valueOf(readTps.get("secondary")));
			}
		}

		if (writeTps != null) {
			if (writeTps.get("y") == null) {
				totalWriteTps.put("y", (float) 0);
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + WRITES
						+ SLASH + "success", "", 0);
			} else {
				totalWriteTps.put("y", totalWriteTps.get("y") + Integer.valueOf(writeTps.get("y")));
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + WRITES
						+ SLASH + "success", "", Integer.valueOf(writeTps.get("y")));
			}
			if (writeTps.get("secondary") == null) {
				totalWriteTps.put("secondary", (float) 0);
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + WRITES
						+ SLASH + "total", "", 0);
			} else {
				totalWriteTps.put("secondary",
						totalWriteTps.get("secondary") + Integer.valueOf(writeTps.get("secondary")));
				reportMetric(baseThroughputMetric + node.getHost().name + SLASH + THROUGHPUT_STATS + SLASH + WRITES
						+ SLASH + "total", "", Integer.valueOf(writeTps.get("secondary")));
			}
		}
	}

	/**
	 * Method to report Summary metric
	 * 
	 * @param totalMemory
	 * @param totalDisk
	 * @param totalUsedMemory
	 * @param batchInitiate
	 * @param totalUSedDisk
	 */
	public void reportSummaryMetric(Float totalUsedMemory, Float totalMemory, Float totalUsedDisk, Float totalDisk,
			Float batchInitiate) {
		String baseSummaryMetric = METRIC_BASE_NAME + SLASH + SUMMARY + SLASH;
		Node node = base.getAerospikeNode(host);
		if (node != null) {
			Map<String, String> nodeStats = base.getNodeStatistics(node);
			if (nodeStats != null) {
				reportMetric(baseSummaryMetric + "cluster_size", "", Float.parseFloat(nodeStats.get("cluster_size")));
			}
		}

		reportMetric(baseSummaryMetric + "used_memory", "", totalUsedMemory);
		reportMetric(baseSummaryMetric + "total_memory", "", totalMemory);

		reportMetric(baseSummaryMetric + "used_disk", "", totalUsedDisk);
		reportMetric(baseSummaryMetric + "total_disk", "", totalDisk);

		reportMetric(baseSummaryMetric + "batch_initiate", "", batchInitiate);

	}

	/**
	 * Method to report cluster-wide reads and writes under Summary metric
	 * 
	 */
	public void reportTotalTps() {
		String baseThroughputMetric = METRIC_BASE_NAME + SLASH + SUMMARY;
		reportMetric(baseThroughputMetric + SLASH + READS + SLASH + "success", "", totalReadTps.get("y"));
		reportMetric(baseThroughputMetric + SLASH + READS + SLASH + "total", "", totalReadTps.get("secondary"));

		reportMetric(baseThroughputMetric + SLASH + WRITES + SLASH + "success", "", totalWriteTps.get("y"));
		reportMetric(baseThroughputMetric + SLASH + WRITES + SLASH + "total", "", totalWriteTps.get("secondary"));
	}

	/**
	 * A method to report node latency for reads, writes_master, proxy, udf and
	 * query.
	 */
	public void reportNodeLatency() {
		String baseLatentyMetric = metricBaseName + SLASH;
		/* setting default values to cluster-wide latency map */
		initClusterWideLatency();
		Node[] nodes = base.getAerospikeNodes(this.host);
		for (Node node : nodes) {
			Map<String, Map<String, String>> latency = base.getNodeLatency(node);
			for (Map.Entry<String, Map<String, String>> entry : latency.entrySet()) {
				String key = entry.getKey();
				for (Map.Entry<String, String> dataEntry : entry.getValue().entrySet()) {
					reportMetric(
							baseLatentyMetric + node.getHost().name + SLASH + LATENCY_STATS + SLASH + key + "/"
									+ dataEntry.getKey() + "/value",
							"", Float.parseFloat(dataEntry.getValue().split(";")[0]));
					reportMetric(
							baseLatentyMetric + node.getHost().name + SLASH + LATENCY_STATS + SLASH + key + "/"
									+ dataEntry.getKey() + "/pct",
							"", Float.parseFloat(dataEntry.getValue().split(";")[1]));

					/* calculating cluster-wide latency */

					calculateClusterWideLatency(key, dataEntry.getKey(),
							Float.parseFloat(dataEntry.getValue().split(";")[0]));

				}
			}
		}
		/* reporting cluster-wide latency */
		reportClusterWideLatency(nodes.length);
	}

	/**
	 * Method to set default values to cluster-wide latency map.
	 * 
	 */
	private void initClusterWideLatency() {
		clusterWideLatency.clear();
		for (String category : LATENCY_CATEGORY) {
			Map<String, Float> bucketMap = new HashMap<String, Float>();
			for (String bucket : LATENCY_BUCKETS) {
				bucketMap.put(bucket, (float) 0);
			}
			clusterWideLatency.put(category, bucketMap);
		}
	}

	/**
	 * Method to calculate cluster-wide latency
	 * 
	 * @param category
	 * @param bucket
	 * @param bucketValue
	 */
	private void calculateClusterWideLatency(String category, String bucket, float bucketValue) {
		float value = clusterWideLatency.get(category).get(bucket) + bucketValue;
		clusterWideLatency.get(category).put(bucket, value);
	}

	/**
	 * Method to report cluster-wide latency
	 * 
	 * @param clusterSize
	 */
	private void reportClusterWideLatency(int clusterSize) {
		String baseClusterWideLatencyMetric = METRIC_BASE_NAME + SLASH + SUMMARY;
		for (Map.Entry<String, Map<String, Float>> entry : clusterWideLatency.entrySet()) {
			String key = entry.getKey();
			for (Map.Entry<String, Float> dataEntry : entry.getValue().entrySet()) {
				if (key.equals(LATENCY_CATEGORY[2])) {
					/* calculate average for query category */
					if (clusterSize > 0)
						reportMetric(baseClusterWideLatencyMetric + SLASH + LATENCY + SLASH + key + SLASH
								+ dataEntry.getKey() + SLASH + "value", "", dataEntry.getValue() / clusterSize);
				} else {
					reportMetric(baseClusterWideLatencyMetric + SLASH + LATENCY + SLASH + key + SLASH
							+ dataEntry.getKey() + SLASH + "value", "", dataEntry.getValue());
				}

			}
		}
	}

	/**
	 * Method to report namespace statistics.
	 * 
	 * @param namespace
	 *            namespace name to fetch the statistics
	 */
	public void reportNamespaceStats(String namespace) {
		String namespaceBaseMatric = metricBaseName + SLASH;
		Node[] nodes = base.getAerospikeNodes(this.host);
		for (Node node : nodes) {
			Map<String, String> namespaceStats = base.getNamespaceStatistics(namespace, node);
			if (namespaceStats != null && namespaceStats.size() != 0) {
				String namespacePrefix = namespaceBaseMatric + node.getHost().name + SLASH + namespace + SLASH;
				for (Map.Entry<String, String> stat : namespaceStats.entrySet()) {
					reportMetric(namespacePrefix + NAMESPACE_STATS + SLASH + stat.getKey(), "",
							Float.parseFloat(stat.getValue()));
				}
			}
		}
	}

	/**
	 * A method to submit Aerospike metrics to New Relic, periodically.
	 * 
	 */
	@Override
	public void pollCycle() {
		try {
			Map<String, Map<String, String>> perNodeStats = reportNodeStatistics();
			Main.setStatistcs(perNodeStats);

			reportNodeLatency();
			String[] namespaces = base.getNamespaces(host);

			if (namespaces.length != 0) {
				for (String namespace : namespaces) {
					reportNamespaceStats(namespace);
				}
			}
		} catch (Connection connection) {
			logger.error("Exception : " + connection.getMessage());
			logger.error("Plugin is getting stoped");
			System.exit(-1);
		} catch (AerospikeException aerospikeException) {
			logger.error("Exception : " + aerospikeException.getMessage());
		} catch (ArrayIndexOutOfBoundsException arrayIndexOutOfBoundsException) {
			logger.info("Server is starting");
		} catch (Exception exception) {
			logger.error("Exception : " + exception);
		}
	}
}
