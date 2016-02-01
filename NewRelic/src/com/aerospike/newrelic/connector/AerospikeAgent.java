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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.Host;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.newrelic.utils.Utils;
import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.configuration.ConfigurationException;
import com.newrelic.metrics.publish.util.Logger;
//import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;

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
	private ArrayList<Host> host_list;
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
	public AerospikeAgent(ArrayList<String> seed_list, String user, String password, String clusterName)
			throws ConfigurationException {
		super(GUID, VERSION);
		try {
			this.user = user;
			this.password = password;
			this.clusterName = clusterName;
			this.metricBaseName = METRIC_BASE_NAME;
			
			/* creating host list from seed list */
			this.host_list = new ArrayList<Host>();
			for (String seed : seed_list) {
				String[] host_port = seed.split(":");
				Host host = new Host(host_port[0], Integer.parseInt(host_port[1]));
				this.host_list.add(host);
			}
			
			/* creating map for cluster-wide reads and writes */
			totalReadTps = new HashMap<String, Float>();
			totalWriteTps = new HashMap<String, Float>();

			/* creating map for cluster-wide latency */
			clusterWideLatency = new HashMap<String, Map<String, Float>>();

			/* Creating AerospikeClient */
			this.base = new Base();
			this.base.createAerospikeClient(this.host_list, this.user, this.password);

			/* Set default values to readTpsHistory and writeTpsHistory */
			setDefaultsToTpsHistory();

			//logger.info("Aerospike Agent initialized: ", formatAgentParams(host, port, user, password, clusterName));
			logger.info("Aerospike Agent initialized: ", formatAgentParams(seed_list, user, password, clusterName));


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

	private String formatAgentParams(ArrayList<String> seed_list, String user, String password, String clusterName) {
		StringBuilder builder = new StringBuilder();
		builder.append("seed_list").append(seed_list.toString()).append(" | ");
		
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
		Long timeStamp = System.currentTimeMillis() / 1000l;
		Node[] nodes = base.getAerospikeNodes();

		for (Node node : nodes) {
			Map<String, String> readTpsHistory = new HashMap<String, String>();
			readTpsHistory.put("timeStamp", Long.toString(timeStamp));
			readTpsHistory.put("totalTps", null);
			readTpsHistory.put("successTps", null);

			Main.readTpsHistory.put(node.getHost().name, readTpsHistory);

			Map<String, String> writeTpsHistory = new HashMap<String, String>();
			writeTpsHistory.put("timeStamp", Long.toString(timeStamp));
			writeTpsHistory.put("totalTps", null);
			writeTpsHistory.put("successTps", null);

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
		logger.debug("Reporting node stats.");
		
		String nodeStatPrefix = metricBaseName + SLASH + NODE_STATS + SLASH;
		Map<String, Map<String, String>> perNodeStats = new HashMap<String, Map<String, String>>();
		Float totalUsedMemory = (float) 0;
		Float totalMemory = (float) 0;
		Float totalUsedDisk = (float) 0;
		Float totalDisk = (float) 0;
		Float batchInitiate = (float) 0;

		/* set default values for cluster-wide TPS */
		initTps();

		Node[] nodes = base.getAerospikeNodes();
		for (Node node : nodes) {
			Map<String, String> nodeStats = base.getNodeStatistics(node);

			if (nodeStats != null) {
				totalUsedMemory += Float.parseFloat(nodeStats.get("used-bytes-memory"));
				totalMemory += Float.parseFloat(nodeStats.get("total-bytes-memory"));
				totalUsedDisk += Float.parseFloat(nodeStats.get("used-bytes-disk"));
				totalDisk += Float.parseFloat(nodeStats.get("total-bytes-disk"));
				batchInitiate += Float.parseFloat(nodeStats.get("batch_initiate"));

				for (Map.Entry<String, String> nodeStat : nodeStats.entrySet()) {
					String metric_name = nodeStatPrefix + node.getHost().name + SLASH + nodeStat.getKey();
					float value = Float.parseFloat(nodeStat.getValue());
					reportMetric(metric_name, "", value);
					logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + value);
				}
			}

			Map<String, String> memoryStats = base.getMemoryStats(nodeStats);
			Map<String, String> diskStats = base.getDiskStats(nodeStats);

			if (Utils.isValidNumber(diskStats.get("free-bytes-disk"))) {				
				String metric_name = nodeStatPrefix + node.getHost().name + "/free-bytes-disk";
				float value = Float.parseFloat(diskStats.get("free-bytes-disk"));
				reportMetric(metric_name, "", value);
				logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + value);
			}

			if (Utils.isValidNumber(diskStats.get("total-bytes-disk"))) {
				String metric_name = nodeStatPrefix + node.getHost().name + "/total-bytes-disk";
				float value = Float.parseFloat(diskStats.get("total-bytes-disk"));
				reportMetric(metric_name, "", value);
				logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + value);
			}

			if (Utils.isValidNumber(memoryStats.get("free-bytes-memory"))) {
				String metric_name = nodeStatPrefix + node.getHost().name + "/free-bytes-memory";
				float value = Float.parseFloat(memoryStats.get("free-bytes-memory"));
				reportMetric(metric_name, "", value);
				logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + value);
			}

			if (Utils.isValidNumber(memoryStats.get("total-bytes-memory"))) {
				String metric_name = nodeStatPrefix + node.getHost().name + "/total-bytes-memory";
				float value = Float.parseFloat(memoryStats.get("total-bytes-memory"));
				reportMetric(metric_name, "", value);
				logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + value);
			}

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
		totalReadTps.put("successTps", (float) 0);
		totalReadTps.put("totalTps", (float) 0);

		totalWriteTps.clear();
		totalWriteTps.put("successTps", (float) 0);
		totalWriteTps.put("totalTps", (float) 0);
	}

	/**
	 * Method to report node throughput.
	 * 
	 * @param nodeStats
	 *            Map of node statistics
	 */
	public void reportThroughput(Map<String, String> nodeStats, Node node) {
		logger.debug("Report node throughput.");
		Map<String, Map<String, String>> tps = base.getThroughput(nodeStats, node);
		Map<String, String> readTps = tps.get("reads");
		Map<String, String> writeTps = tps.get("writes");

		String baseThroughputMetric = metricBaseName + SLASH + THROUGHPUT_STATS + SLASH;
		if (readTps != null) {
			String read_metric_prefix = baseThroughputMetric + node.getHost().name + SLASH + READS + SLASH;
			if (readTps.get("successTps") == null) {
				totalReadTps.put("successTps", (float) 0);
				reportMetric(read_metric_prefix + "success", "", 0);
				logger.debug("Reprting metics, metric name: " + read_metric_prefix + "success" + ", value: " + 0);

			} else {
				totalReadTps.put("successTps", totalReadTps.get("successTps") + Integer.valueOf(readTps.get("successTps")));
				reportMetric(read_metric_prefix + "success", "", Integer.valueOf(readTps.get("successTps")));
				logger.debug("Reprting metics, metric name: " + read_metric_prefix + "success" + ", value: " + Integer.valueOf(readTps.get("successTps")));
				
			}
			
			if (readTps.get("totalTps") == null) {
				totalReadTps.put("totalTps", (float) 0);
				reportMetric(read_metric_prefix + "total", "", 0);
				logger.debug("Reprting metics, metric name: " + read_metric_prefix + "total" + ", value: " + 0);

			} else {
				totalReadTps.put("totalTps", totalReadTps.get("totalTps") + Integer.valueOf(readTps.get("totalTps")));
				reportMetric(read_metric_prefix + "total", "", Integer.valueOf(readTps.get("totalTps")));
				logger.debug("Reprting metics, metric name: " + read_metric_prefix + "total" + ", value: " + Integer.valueOf(readTps.get("totalTps")));

			}
		}

		if (writeTps != null) {
			String write_metric_name = baseThroughputMetric + node.getHost().name + SLASH + WRITES + SLASH;
			if (writeTps.get("successTps") == null) {
				totalWriteTps.put("successTps", (float) 0);
				reportMetric(write_metric_name + "success", "", 0);
				logger.debug("Reprting metics, metric name: " + write_metric_name + "success" + ", value: " + 0);

			} else {
				totalWriteTps.put("successTps", totalWriteTps.get("successTps") + Integer.valueOf(writeTps.get("successTps")));
				reportMetric(write_metric_name + "success", "", Integer.valueOf(writeTps.get("successTps")));
				logger.debug("Reprting metics, metric name: " + write_metric_name + "success" + ", value: " + Integer.valueOf(writeTps.get("successTps")));

			}
			if (writeTps.get("totalTps") == null) {
				totalWriteTps.put("totalTps", (float) 0);
				reportMetric(write_metric_name + "total", "", 0);
				logger.debug("Reprting metics, metric name: " + write_metric_name + "total" + ", value: " + 0);

			} else {
				totalWriteTps.put("totalTps",totalWriteTps.get("totalTps") + Integer.valueOf(writeTps.get("totalTps")));
				reportMetric(write_metric_name + "total", "", Integer.valueOf(writeTps.get("totalTps")));
				logger.debug("Reprting metics, metric name: " + write_metric_name + "total" + ", value: " + Integer.valueOf(writeTps.get("totalTps")));

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
		logger.debug("Reporting summary metric.");
		String baseSummaryMetric = METRIC_BASE_NAME + SLASH + SUMMARY + SLASH;
		/* Getting one active node from cluster and getting its stats*/
		Node node = base.getAerospikeNodes()[0];
		if (node != null) {
			Map<String, String> nodeStats = base.getNodeStatistics(node);
			if (nodeStats != null) {
				reportMetric(baseSummaryMetric + "cluster_size", "", Float.parseFloat(nodeStats.get("cluster_size")));
				logger.debug("Reprting metics, metric name: " + baseSummaryMetric + "cluster_size" + ", value: " + Float.parseFloat(nodeStats.get("cluster_size")));
			}
		}

		reportMetric(baseSummaryMetric + "used_memory", "", totalUsedMemory);
		logger.debug("Reprting metics, metric name: " + baseSummaryMetric + "used_memory" + ", value: " + totalUsedMemory);

		reportMetric(baseSummaryMetric + "total_memory", "", totalMemory);
		logger.debug("Reprting metics, metric name: " + baseSummaryMetric + "total_memory" + ", value: " + totalMemory);

		reportMetric(baseSummaryMetric + "used_disk", "", totalUsedDisk);
		logger.debug("Reprting metics, metric name: " + baseSummaryMetric + "used_disk" + ", value: " + totalUsedDisk);
		
		reportMetric(baseSummaryMetric + "total_disk", "", totalDisk);
		logger.debug("Reprting metics, metric name: " + baseSummaryMetric + "total_disk" + ", value: " + totalDisk);

		reportMetric(baseSummaryMetric + "batch_initiate", "", batchInitiate);
		logger.debug("Reprting metics, metric name: " + baseSummaryMetric + "batch_initiate" + ", value: " + batchInitiate);

	}

	/**
	 * Method to report cluster-wide reads and writes under Summary metric
	 * 
	 */
	public void reportTotalTps() {
		logger.debug("Report total tps.");
		String baseThroughputMetric = METRIC_BASE_NAME + SLASH + SUMMARY + SLASH;
		String read_metric = baseThroughputMetric + READS + SLASH;
		String write_metric = baseThroughputMetric + WRITES + SLASH;
		
		reportMetric(read_metric + "success", "", totalReadTps.get("successTps"));
		logger.debug("Reprting metics, metric name: " + read_metric + ", value: " + totalReadTps.get("successTps"));

		reportMetric(read_metric + "total", "", totalReadTps.get("totalTps"));
		logger.debug("Reprting metics, metric name: " + read_metric + ", value: " + totalReadTps.get("totalTps"));

		reportMetric(write_metric + "success", "", totalWriteTps.get("successTps"));
		logger.debug("Reprting metics, metric name: " + write_metric + ", value: " + totalWriteTps.get("successTps"));

		reportMetric(write_metric + "total", "", totalWriteTps.get("totalTps"));
		logger.debug("Reprting metics, metric name: " + write_metric + ", value: " + totalWriteTps.get("totalTps"));

	}

	/**
	 * A method to report node latency for reads, writes_master, proxy, udf and
	 * query.
	 */
	public void reportNodeLatency() {
		logger.debug("Reporting node latency.");
		String baseLatentyMetric = metricBaseName + SLASH + LATENCY_STATS + SLASH;
		/* setting default values to cluster-wide latency map */
		initClusterWideLatency();
		Node[] nodes = base.getAerospikeNodes();
		for (Node node : nodes) {
			Map<String, Map<String, String>> latency = base.getNodeLatency(node);
			for (Map.Entry<String, Map<String, String>> entry : latency.entrySet()) {
				String key = entry.getKey();
				for (Map.Entry<String, String> dataEntry : entry.getValue().entrySet()) {
					
					String metric_name_prefix = baseLatentyMetric + node.getHost().name + SLASH + key + "/" + dataEntry.getKey();					
					float metric_value = Float.parseFloat(dataEntry.getValue().split(";")[0]);
					float metric_pct = Float.parseFloat(dataEntry.getValue().split(";")[1]);
					
					reportMetric(metric_name_prefix + "/value", "", metric_value);
					logger.debug("Reprting metics, metric name: " + metric_name_prefix + "/value" + ", value: " + metric_value);

					reportMetric(metric_name_prefix + "/pct", "", metric_pct);
					logger.debug("Reprting metics, metric name: " + metric_name_prefix + "/pct" + ", value: " + metric_pct);

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
		logger.debug("Calculating clusterwide latency.");
		float value = clusterWideLatency.get(category).get(bucket) + bucketValue;
		clusterWideLatency.get(category).put(bucket, value);
	}

	/**
	 * Method to report cluster-wide latency
	 * 
	 * @param clusterSize
	 */
	private void reportClusterWideLatency(int clusterSize) {
		logger.debug("Reporting clusterwide latency.");
		String baseClusterWideLatencyMetric = METRIC_BASE_NAME + SLASH + SUMMARY + SLASH + LATENCY + SLASH;
		for (Map.Entry<String, Map<String, Float>> entry : clusterWideLatency.entrySet()) {
			String key = entry.getKey();
			for (Map.Entry<String, Float> dataEntry : entry.getValue().entrySet()) {
				String metric_name = baseClusterWideLatencyMetric + key + SLASH + dataEntry.getKey() + SLASH + "value";
				
				if (key.equals(LATENCY_CATEGORY[2])) {
					/* calculate average for query category */
					if (clusterSize > 0) {
						reportMetric(metric_name, "", dataEntry.getValue() / clusterSize);
						logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + dataEntry.getValue() / clusterSize);
					}
					
				} else {
					reportMetric(metric_name, "", dataEntry.getValue());
					logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + dataEntry.getValue());

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
		logger.debug("Reporting namespace stats. Namespace: ", namespace);
		String namespaceBaseMatric = metricBaseName + SLASH;
		Node[] nodes = base.getAerospikeNodes();
		for (Node node : nodes) {
			Map<String, String> namespaceStats = base.getNamespaceStatistics(namespace, node);
			if (namespaceStats != null && namespaceStats.size() != 0) {
				String namespacePrefix = namespaceBaseMatric + NAMESPACE_STATS + SLASH + node.getHost().name + SLASH + namespace + SLASH;
				for (Map.Entry<String, String> stat : namespaceStats.entrySet()) {
					String metric_name = namespacePrefix + stat.getKey();
					float value = Float.parseFloat(stat.getValue());
					reportMetric(metric_name, "",value);
					logger.debug("Reprting metics, metric name: " + metric_name + ", value: " + value);
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
			logger.info("********** Reporting stats for cluster: ", this.clusterName + " **********");
			Map<String, Map<String, String>> perNodeStats = reportNodeStatistics();
			Main.setStatistcs(perNodeStats);

			reportNodeLatency();
			
			String[] namespaces = base.getNamespaces();
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
