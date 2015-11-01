package com.aerospike.newrelic.connector;

import static com.aerospike.newrelic.utils.Constants.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

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
			this.metricBaseName = METRIC_BASE_NAME;

			// Creating AerospikeClient
			this.base = new Base();
			this.base.createAerospikeClient(this.host, this.port, this.user, this.password);

			// Set default values to readTpsHistory and writeTpsHistory
			setDefaultsToTpsHistory();

		} catch (Exception exception) {
			logger.error("Error reading configuration parameters : ", exception);
			throw new ConfigurationException("Error reading configuration parameters...", exception);
		}
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
	 * Method to report Summary metric
	 * 
	 * @param totalMemory
	 * @param totalDisk
	 * @param totalUsedMemory
	 * @param totalUSedDisk
	 */
	public void reportSummaryMetric(Float totalUsedMemory, Float totalMemory, Float totalUsedDisk, Float totalDisk) {
		String baseSummaryMetric = metricBaseName + SLASH + SUMMARY + SLASH;
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
	}

	/**
	 * Method to fetch node statistics from base and report to new relic.
	 * 
	 * @return Map<String, String> A map of node statistics
	 */
	public Map<String, Map<String, String>> reportNodeStatistics() {
		String nodeStatPrefix = metricBaseName + SLASH + NODE + SLASH;
		Map<String, Map<String, String>> perNodeStats = new HashMap<String, Map<String, String>>();

		Float totalUsedMemory = (float) 0;
		Float totalMemory = (float) 0;

		Float totalUsedDisk = (float) 0;
		Float totalDisk = (float) 0;

		Node[] nodes = base.getAerospikeNodes(this.host);
		for (Node node : nodes) {
			Map<String, String> nodeStats = base.getNodeStatistics(node);

			if (nodeStats != null) {
				totalUsedMemory += Float.parseFloat(nodeStats.get("used-bytes-memory"));
				totalMemory += Float.parseFloat(nodeStats.get("total-bytes-memory"));

				totalUsedDisk += Float.parseFloat(nodeStats.get("used-bytes-disk"));
				totalDisk += Float.parseFloat(nodeStats.get("total-bytes-disk"));

				for (Map.Entry<String, String> nodeStat : nodeStats.entrySet()) {
					reportMetric(nodeStatPrefix + node.getHost().name + SLASH + nodeStat.getKey(), "",
							Float.parseFloat(nodeStat.getValue()));
				}
			}

			Map<String, String> memoryStats = base.getMemoryStats(nodeStats);
			Map<String, String> diskStats = base.getDiskStats(nodeStats);

			if (Utils.validNumber(diskStats.get("free-bytes-disk")))
				reportMetric(nodeStatPrefix + node.getHost().name + "/disk_usage_free", "",
						Float.parseFloat(diskStats.get("free-bytes-disk")));

			if (Utils.validNumber(diskStats.get("total-bytes-disk")))
				reportMetric(nodeStatPrefix + node.getHost().name + "/disk_usage_total", "",
						Float.parseFloat(diskStats.get("total-bytes-disk")));

			if (Utils.validNumber(memoryStats.get("free-bytes-memory")))
				reportMetric(nodeStatPrefix + node.getHost().name + "/memory_usage_free", "",
						Float.parseFloat(memoryStats.get("free-bytes-memory")));

			if (Utils.validNumber(memoryStats.get("total-bytes-memory")))
				reportMetric(nodeStatPrefix + node.getHost().name + "/memory_usage_total", "",
						Float.parseFloat(memoryStats.get("total-bytes-memory")));

			perNodeStats.put(node.getHost().name, nodeStats);
			/* Report the throughput metrics */
			reportThroughput(nodeStats, node);
		}
		// report summary metric
		reportSummaryMetric(totalUsedMemory, totalMemory, totalUsedDisk, totalDisk);
		return perNodeStats;
	}

	/**
	 * A method to report node latency for reads, writes_master, proxy, udf and
	 * query.
	 */
	public void reportNodeLatency() {
		String baseLatentyMetric = metricBaseName + SLASH + LATENCY + SLASH;
		Node[] nodes = base.getAerospikeNodes(this.host);
		for (Node node : nodes) {
			Map<String, Map<String, String>> latency = base.getNodeLatency(node);
			for (Map.Entry<String, Map<String, String>> entry : latency.entrySet()) {
				String key = entry.getKey();
				for (Map.Entry<String, String> dataEntry : entry.getValue().entrySet()) {
					if (dataEntry.getKey().equals("opsPerSec")) {
						reportMetric(baseLatentyMetric + node.getHost().name + SLASH + key + "/" + dataEntry.getKey()
								+ "/value", "", Float.parseFloat(dataEntry.getValue()));
					} else {
						reportMetric(baseLatentyMetric + node.getHost().name + SLASH + key + "/" + dataEntry.getKey()
								+ "/value", "", Float.parseFloat(dataEntry.getValue().split(";")[0]));
						reportMetric(baseLatentyMetric + node.getHost().name + SLASH + key + "/" + dataEntry.getKey()
								+ "/pct", "", Float.parseFloat(dataEntry.getValue().split(";")[1]));
					}
				}
			}
		}
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
		String baseThroughputMetric = metricBaseName + SLASH + THROUGHPUT + SLASH;
		if (readTps != null && readTps.get("y") == null)
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + READS + SLASH + "success", "", 0);
		else
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + READS + SLASH + "success", "",
					Integer.valueOf(readTps.get("y")));
		if (readTps.get("secondary") == null)
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + READS + SLASH + "total", "", 0);
		else
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + READS + SLASH + "total", "",
					Integer.valueOf(readTps.get("secondary")));

		if (writeTps.get("y") == null)
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + WRITES + SLASH + "success", "", 0);
		else
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + WRITES + SLASH + "success", "",
					Integer.valueOf(writeTps.get("y")));
		if (writeTps.get("secondary") == null)
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + WRITES + SLASH + "total", "", 0);
		else
			reportMetric(baseThroughputMetric + node.getHost().name + SLASH + WRITES + SLASH + "total", "",
					Integer.valueOf(writeTps.get("secondary")));
	}

	/**
	 * Method to report namespace statistics.
	 * 
	 * @param namespace
	 *            namespace name to fetch the statistics
	 */
	public void reportNamespaceStats(String namespace) {
		String namespaceBaseMatric = metricBaseName + SLASH + NAMESPACE + SLASH;
		Node[] nodes = base.getAerospikeNodes(this.host);
		for (Node node : nodes) {
			Map<String, String> namespaceStats = base.getNamespaceStatistics(namespace, node);
			if (namespaceStats != null) {
				String namespacePrefix = namespaceBaseMatric + node.getHost().name + SLASH + namespace + SLASH;
				for (Map.Entry<String, String> stat : namespaceStats.entrySet()) {
					reportMetric(namespacePrefix + stat.getKey(), "", Float.parseFloat(stat.getValue()));
				}
			}
		}
	}

	/**
	 * A method to submit Aerospike metrics to New Relic. It is called
	 * periodically
	 */
	@Override
	public void pollCycle() {

		Map<String, Map<String, String>> perNodeStats = reportNodeStatistics();
		Main.setStatistcs(perNodeStats);

		reportNodeLatency();
		String[] namespaces = base.getNamespaces(host);

		if (namespaces.length != 0) {
			for (String namespace : namespaces) {
				reportNamespaceStats(namespace);
			}
		}
	}
}
