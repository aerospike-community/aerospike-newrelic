package com.aerospike.newrelic.connector;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.newrelic.utils.Utils;
import com.newrelic.metrics.publish.util.Logger;

/**
 * Base class to communicate with Aerospike.
 * 
 * @author Aniruddha Atre
 *
 */
public class Base {

	private AerospikeClient client;
	private ClientPolicy policy;

	private static final Logger logger = Logger.getLogger(Base.class);

	/**
	 * Base Constructor
	 */
	/*public Base() {

		this.client = null;
		this.policy = null;
	}*/

	/**
	 * Method to initialize Aerospike client and policy.
	 * 
	 * @param ip
	 *            IP of the Aerospike node
	 * @param port
	 *            Port number of Aeropsike node
	 * @param user
	 *            User name for Aerospike node if security enabled else null
	 * @param password
	 *            Password for Aerospike node if if security enabled else null
	 */
	public void createAerospikeClient(String ip, Integer port, String user, String password) {

		if (this.client == null && this.policy == null) {
			this.policy = new ClientPolicy();
			if (user != null && password != null) {
				this.policy.user = user;
				this.policy.password = password;
			}
			//this.policy.timeout = 85000;
			this.policy.maxThreads = 10;
			this.policy.maxSocketIdle = 10;
			this.client = new AerospikeClient(policy, ip, port);
			if (this.client == null || !this.client.isConnected()) {
				logger.info("ERROR: "
						+ "Connection to Aerospike cluster failed! Please check the server settings and try again!");
			}
		}
	}

	/**
	 * Getter method for Aerospike client.
	 * 
	 * @return Aerospike client
	 */
	public AerospikeClient getAerospikeClient() {
		if (this.client != null)
			return this.client;
		return null;
	}

	/**
	 * Method to get Aerospike node from the client.
	 * 
	 * @param ip
	 *            IP of the Aerospike node
	 * @return Aerospiek Node
	 */
	public Node getAerospikeNode(String ip) {

		if (this.client == null)
			return null;
		Node[] nodes = this.client.getNodes();
		for (Node node : nodes) {
			if (node.getHost().name.equalsIgnoreCase(ip)) {
				return node;
			}
		}
		return null;
	}

	/**
	 * Method to get node statistics from Aerospike
	 * 
	 * @param ip
	 *            IP of the Aerospike node
	 * @param port
	 *            Port number for Aerospike node
	 * @param user
	 *            User name for Aerospike node if security enabled else null
	 * @param password
	 *            Password for Aerospike node if security enabled else null
	 * @return Map<String, String> Map of node statistics
	 */
	public Map<String, String> getNodeStatistics(String ip, Integer port, String user, String password) {

		Map<String, String> nodeStats = null;
		Node node = null;
		if (this.client != null)
			node = getAerospikeNode(ip);
		if (this.client != null && node != null) {
			nodeStats = Info.request(null, node);
			String[] stats = nodeStats.get("statistics").split(";");
			nodeStats.clear();
			for (String stat : stats) {
				String[] pair = stat.split("=");
				pair[1] = Utils.handleAerospikeString(pair[1]);
				if (Utils.validNumber(pair[1])) {
					Float val = Float.parseFloat(pair[1]);
					nodeStats.put(pair[0], String.valueOf(val));
				}
			}
		}
		return nodeStats;
	}

	/**
	 * Method to get namespace statistics from Aerospike.
	 * 
	 * @param namespace
	 *            Namespace name
	 * @param ip
	 *            IP of Aerospike node
	 * @param port
	 *            Port number of Aerospike node
	 * @param user
	 *            User name for Aerospike if security enabled else null
	 * @param password
	 *            Password for Aerospike if security enabled else null
	 * @return Map<String, String> Map of namespace statistics
	 */
	public Map<String, String> getNamespaceStatistics(String namespace, String ip, Integer port, String user,
			String password) {

		Map<String, String> namespaceStats = new HashMap<String, String>();
		Node node = getAerospikeNode(ip);
		String filter = "namespace/" + namespace;
		String[] stats = new String[0];
		if (node != null) {
			stats = Info.request(null, node, filter).split(";");
			for (String stat : stats) {
				String[] pair = stat.split("=");
				pair[1] = Utils.handleAerospikeString(pair[1]);
				if (Utils.validNumber(pair[1])) {
					Float val = Float.parseFloat(pair[1]);
					namespaceStats.put(pair[0], String.valueOf(val));
				}
			}

			namespaceStats.put("used-pct-memory",
					String.valueOf(100 - Float.parseFloat(namespaceStats.get("free-pct-memory"))));
			return namespaceStats;
		}
		return null;
	}

	/**
	 * Method to get node latency from Aerospike
	 * 
	 * @param ip
	 *            IP of Aerospike node
	 * @param port
	 *            Port number of Aerospike node
	 * @param user
	 *            User name for Aerospike node if security enabled else null
	 * @param password
	 *            Password for Aerospike node if security enabled else null
	 * @return
	 */
	public Map<String, Map<String, String>> getNodeLatency(String ip, Integer port, String user, String password) {

		Map<String, Map<String, String>> latency = new HashMap<String, Map<String, String>>();
		Node node = getAerospikeNode(ip);
		String filter = "latency:";
		String latencyString = "";
		String[] latencyBuckets = {};

		if (node != null)
			latencyString = Info.request(null, node, filter);

		if (latencyString.length() != 0)
			latencyBuckets = latencyString.split(";");

		for (Integer i = 0; i < latencyBuckets.length; i += 2) {
			Map<String, String> data = new HashMap<String, String>();
			String line0 = latencyBuckets[i];
			String line1 = latencyBuckets[i + 1];
			String key = line0.substring(0, line0.indexOf(':'));

			if (key.equalsIgnoreCase("writes_reply"))
				continue;

			String[] keys = line0.substring(line0.indexOf('>')).split(",");
			Collections.reverse(Arrays.asList(keys));
			String[] values = line1.substring(line1.indexOf(',') + 1).split(",");
			Float opsPerSec = Float.parseFloat(values[0]);
			Collections.reverse(Arrays.asList(values));

			String previousKey = "";
			Float previousValue = (float) 0.0;
			Float lessThan1;

			if (opsPerSec == (float) 0.0)
				lessThan1 = (float) 0.0;
			else
				lessThan1 = (float) 100.0;
			for (Integer j = 0; j < keys.length; j++) {
				Float pct = Float.parseFloat(values[j]) - previousValue;
				previousValue = Float.parseFloat(values[j]);
				String k = "";
				if (previousKey != "") {
					// unicode for gt == 0x003E && lt == 0x003C
					k = "\u003E " + keys[j].substring(1) + " to \u003C " + previousKey.substring(1);
				} else {
					k = "\u003E " + keys[j].substring(1);
				}
				previousKey = keys[j];
				lessThan1 -= pct;
				Float val = opsPerSec * pct / 100;
				data.put(k, String.valueOf(val) + ";" + String.valueOf(pct));
			}
			Float lessThan1Val = opsPerSec * lessThan1 / 100;
			data.put("\u003C 1ms", String.valueOf(lessThan1Val) + ";" + String.valueOf(lessThan1));
			latency.put(key, data);
		}
		return latency;
	}

	/**
	 * Method to extract throughput from node statistics.
	 * 
	 * @param nodeStats
	 *            Map of node statistics
	 * @return Map<String, String> Map of node throughput(reads and writes)
	 */
	public Map<String, Map<String, String>> getThroughput(Map<String, String> nodeStats) {

		String oldReadReqs, oldReadSuccess, oldWriteReqs, oldWriteSuccess, newReadReqs, newReadSuccess, newWriteReqs,
				newWriteSuccess;

		Map<String, String> writeTpsHistory = new HashMap<String, String>();
		Map<String, String> readTpsHistory = new HashMap<String, String>();
		Map<String, Map<String, String>> output = new HashMap<String, Map<String, String>>();
		if (Main.statsHistory.containsKey("stat_read_reqs"))
			oldReadReqs = Main.statsHistory.get("stat_read_reqs");
		else
			oldReadReqs = "";
		if (Main.statsHistory.containsKey("stat_read_success"))
			oldReadSuccess = Main.statsHistory.get("stat_read_success");
		else
			oldReadSuccess = "";
		if (Main.statsHistory.containsKey("stat_write_reqs"))
			oldWriteReqs = Main.statsHistory.get("stat_write_reqs");
		else
			oldWriteReqs = "";
		if (Main.statsHistory.containsKey("stat_write_success"))
			oldWriteSuccess = Main.statsHistory.get("stat_write_success");
		else
			oldWriteSuccess = "";

		if (nodeStats != null && nodeStats.containsKey("stat_read_reqs"))
			newReadReqs = nodeStats.get("stat_read_reqs");
		else
			newReadReqs = "";
		if (nodeStats != null && nodeStats.containsKey("stat_read_success"))
			newReadSuccess = nodeStats.get("stat_read_success");
		else
			newReadSuccess = "";
		if (nodeStats != null && nodeStats.containsKey("stat_write_reqs"))
			newWriteReqs = nodeStats.get("stat_write_reqs");
		else
			newWriteReqs = "";
		if (nodeStats != null && nodeStats.containsKey("stat_write_success"))
			newWriteSuccess = nodeStats.get("stat_write_success");
		else
			newWriteSuccess = "";
		Integer timestamp = Calendar.getInstance().get(Calendar.MILLISECOND);

		boolean writeCondition = newWriteReqs.isEmpty() || newWriteSuccess.isEmpty() || oldWriteReqs.isEmpty()
				|| oldWriteSuccess.isEmpty();
		boolean readCondition = newReadReqs.isEmpty() || newReadSuccess.isEmpty() || oldReadReqs.isEmpty()
				|| oldReadSuccess.isEmpty();

		if (!writeCondition) {
			Float totalWriteTps = Float.valueOf(newWriteReqs) - Float.valueOf(oldWriteReqs);
			Float successWriteTps = Float.valueOf(newWriteSuccess) - Float.valueOf(oldWriteSuccess);
			Integer oldTimestamp = Integer.valueOf(Main.writeTpsHistory.get("x"));
			Integer timeDifference = timestamp - oldTimestamp;
			totalWriteTps = Math.abs(totalWriteTps / timeDifference);
			successWriteTps = Math.abs(successWriteTps / timeDifference);
			writeTpsHistory.put("x", Integer.toString(timestamp));
			writeTpsHistory.put("secondary", Integer.toString(Math.round(totalWriteTps)));
			writeTpsHistory.put("y", Integer.toString(Math.round(successWriteTps)));
		} else {
			writeTpsHistory.put("x", Integer.toString(timestamp));
			writeTpsHistory.put("secondary", null);
			writeTpsHistory.put("y", null);
		}

		if (!readCondition) {
			Float totalReadTps = Float.valueOf(newReadReqs) - Float.valueOf(oldReadReqs);
			Float successReadTps = Float.valueOf(newReadSuccess) - Float.valueOf(oldReadSuccess);
			Integer oldTimestamp = Integer.valueOf(Main.readTpsHistory.get("x"));
			Integer timeDifference = timestamp - oldTimestamp;
			totalReadTps = Math.abs(totalReadTps / timeDifference);
			successReadTps = Math.abs(successReadTps / timeDifference);
			readTpsHistory.put("x", Integer.toString(timestamp));
			readTpsHistory.put("secondary", Integer.toString(Math.round(totalReadTps)));
			readTpsHistory.put("y", Integer.toString(Math.round(successReadTps)));
		} else {
			readTpsHistory.put("x", Integer.toString(timestamp));
			readTpsHistory.put("secondary", null);
			readTpsHistory.put("y", null);
		}
		Main.setReadTpsHistory(readTpsHistory);
		Main.setWriteTpsHistory(writeTpsHistory);

		output.put("reads", readTpsHistory);
		output.put("writes", writeTpsHistory);
		return output;
	}

	/**
	 * Method to get namespaces for Aerospike node.
	 * 
	 * @param ip
	 *            IP of Aerospike node
	 * @param port
	 *            Port number for Aerospike node
	 * @param user
	 *            User name for Aerospike if security enabled else null
	 * @param password
	 *            Password for Aerospike if security enabled else null
	 * @return String[] Array of Namespaces
	 */
	public String[] getNamespaces(String ip, Integer port, String user, String password) {

		Node node = getAerospikeNode(ip);
		String[] namespaces;
		String filter = "namespaces";
		String ns_str = "";
		if (node != null)
			ns_str = Info.request(null, node, filter);
		namespaces = ns_str.split(";");
		for (String ns : namespaces)
			getNamespaceStatistics(ns, ip, port, user, password);
		return namespaces;
	}

	/**
	 * Method to extract memory stat metrics from node statistics.
	 * 
	 * @param nodeStats
	 *            Map of node statistics
	 * @return Map<String, String> Map of memory statistics
	 */
	public Map<String, String> getMemoryStats(Map<String, String> nodeStats) {

		Map<String, String> memoryStats = new HashMap<String, String>();
		try {
			memoryStats.put("used-bytes-memory", nodeStats.get("used-bytes-memory"));
			memoryStats.put("total-bytes-memory", nodeStats.get("total-bytes-memory"));
			Float freeBytes = (Float.parseFloat(nodeStats.get("total-bytes-memory"))
					- Float.parseFloat(nodeStats.get("used-bytes-memory")));
			memoryStats.put("free-bytes-memory", Float.toString(freeBytes));
		} catch (Exception exception) {
			memoryStats.put("free-bytes-memory", "n/s");
			memoryStats.put("total-bytes-memory", "n/s");
			logger.error("ERROR: ", exception);
		}

		return memoryStats;
	}

	/**
	 * Method to extract disk stats from node statistics.
	 * 
	 * @param nodeStats
	 *            Map of node statistics
	 * @return Map<String, String> Map of Disk statistics
	 */
	public Map<String, String> getDiskStats(Map<String, String> nodeStats) {

		Map<String, String> diskStats = new HashMap<String, String>();
		try {
			diskStats.put("used-bytes-disk", nodeStats.get("used-bytes-disk"));
			diskStats.put("total-bytes-disk", nodeStats.get("total-bytes-disk"));
			Float freeBytes = (Float.parseFloat(nodeStats.get("total-bytes-disk"))
					- Float.parseFloat(nodeStats.get("used-bytes-disk")));
			diskStats.put("free-bytes-disk", Float.toString(freeBytes));
		} catch (Exception exception) {
			diskStats.put("free-bytes-disk", "n/s");
			diskStats.put("total-bytes-disk", "n/s");
			logger.error("ERROR: ", exception);
		}
		return diskStats;
	}

	/**
	 * Method to close all connection to Aerospike server.
	 * 
	 */
	public void closeClientConnections() {
		if (this.client != null)
			this.client.close();
	}
}