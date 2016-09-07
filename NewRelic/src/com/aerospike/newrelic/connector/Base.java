package com.aerospike.newrelic.connector;

import static com.aerospike.newrelic.utils.Constants.LATENCY_ERROR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.newrelic.utils.Utils;
import com.newrelic.metrics.publish.util.Logger;
import com.aerospike.client.Host;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Base class to communicate with Aerospike.
 * 
 *
 */
public class Base {

	private AerospikeClient client;
	private ClientPolicy policy;

	private static final Logger logger = Logger.getLogger(Base.class);

	/**
	 * Method to initialize Aerospike client and policy.
	 * 
	 * @param host_list
	 *            Host list to create java Aerospike client object.
	 * @param user
	 *            User name for Aerospike node if security enabled else null
	 * @param password
	 *            Password for Aerospike node if if security enabled else null
	 */
	
	public void createAerospikeClient(ArrayList<Host> host_list, String user, String password) {
		if (this.client == null && this.policy == null) {
			this.policy = new ClientPolicy();
			if (user != null && password != null) {
				this.policy.user = user;
				this.policy.password = password;
			}
			// this.policy.timeout = 85000;
			this.policy.maxThreads = 10;
			this.policy.maxSocketIdle = 10;
			Host []host_array = new Host[host_list.size()];
			host_list.toArray(host_array);
			this.client = new AerospikeClient(policy, host_array);
			if (this.client == null || !this.client.isConnected()) {
				logger.error("Connection to Aerospike cluster failed! Please check the server settings and try again!");
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
	 * Method to get Aerospike nodes from the client.
	 * 
	 * @return Aerospiek Nodes
	 */
	public Node[] getAerospikeNodes() {
		logger.debug("Getting aerospike nodes");
		if (this.client == null)
			return null;
		return this.client.getNodes();
	}


	/**
	 * Method to get node statistics from Aerospike
	 * 
	 * @param node
	 *            Aerospike node
	 * @return Map<String, String> Map of node statistics
	 */
	public Map<String, String> getNodeStatistics(Node node) {
		logger.debug("Getting node statistics for node: " + node.toString());
		Map<String, String> nodeStats = null;
		if (this.client != null && node != null) {
			nodeStats = Info.request(null, node);
			String[] stats = nodeStats.get("statistics").split(";");
			nodeStats.clear();
			for (String stat : stats) {
				String[] pair = stat.split("=");
				pair[1] = Utils.transformStringMetric(pair[1]);
				if (Utils.isValidNumber(pair[1])) {
					Float val = Float.parseFloat(pair[1]);
					// Older version(<3.9) have mixed stats(contains [-] and [_] both.)
					// Convert all [-] to [_].
					pair[0] = pair[0].replace("-", "_");
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
	 * @param node
	 *            Aerospike node
	 * @return Map<String, String> Map of namespace statistics
	 */
	public Map<String, String> getNamespaceStatistics(String namespace, Node node) {

		Map<String, String> namespaceStats = new HashMap<String, String>();
		String filter = "namespace/" + namespace;
		String[] stats = new String[0];
		if (node != null) {
			stats = Info.request(null, node, filter).split(";");
			for (String stat : stats) {
				String[] pair = stat.split("=");
				pair[1] = Utils.transformStringMetric(pair[1]);
				if (Utils.isValidNumber(pair[1])) {
					Float val = Float.parseFloat(pair[1]);
					// Older version(<3.9) have mixed stats(contains [-] and [_] both.)
					// Convert all [-] to [_].
					pair[0] = pair[0].replace("-", "_");					
					namespaceStats.put(pair[0], String.valueOf(val));
				}
			}
			return namespaceStats;
		}
		return null;
	}

	/**
	 * Method to get node latency from Aerospike
	 * 
	 * @param node
	 *            Aerospike node
	 * @return Map<String, Map<String, String>> Map of latency stats
	 */
	public Map<String, Map<String, String>> getNodeLatency(Node node) {
		logger.debug("Get node latency for node: " + node);
		Map<String, Map<String, String>> latency = new HashMap<String, Map<String, String>>();
		String filter = "latency:";
		String latencyString = "";
		String[] latencyBuckets = {};
		
		latencyString = Info.request(null, node, filter);
		logger.debug("latency_info: " + latencyString);


		if (latencyString.length() != 0 && !latencyString.contains(LATENCY_ERROR))
			latencyBuckets = latencyString.split(";");

		for (Integer i = 0; i < latencyBuckets.length; i += 2) {
			Map<String, String> data = new HashMap<String, String>();
			String line0 = latencyBuckets[i];
			if (line0.contains("no-data")) {
				logger.error("Not enough info for latency: error-run-too-short-or-back-too-small. ->" + line0);
				continue;
			}
			String line1 = latencyBuckets[i + 1];
			String key = line0.substring(0, line0.indexOf(':'));

			// 3.9+ server send namespace level latency in. {test}-read
			// Change this in form of read-{test}. namespace name is unpredictable. put unpredictable part at last.
			if (key.contains("reads"))
				key = "read";
			
			if (key.contains("writes"))
				key = "write";
			
            boolean newAsd = newAsdversion(node);
            if (newAsd == true) {
				String[] lst = key.split("-");
				if (lst.length > 1) {
					key = lst[1] + "-" + lst[0];
				}
            }
			
			if (key.contains("writes_reply"))
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
					
					k = keys[j].substring(1) +  "_to_" + previousKey.substring(1);
					 
				} else {
					
					k = "GT_" + keys[j].substring(1);
				}
				previousKey = keys[j]; 
				
				lessThan1 -= pct;
				Float val = opsPerSec * pct / 100;
				//Float val = Float.parseFloat("35000.0");
				data.put(k, String.valueOf(val) + ";" + String.valueOf(pct));
			}
			Float lessThan1Val = opsPerSec * lessThan1 / 100;
			data.put("0ms_to_1ms", String.valueOf(lessThan1Val) + ";" + String.valueOf(lessThan1));

			/*
			 * data.put("\u003C 1ms", String.valueOf(lessThan1Val) + ";" +
			 * String.valueOf(lessThan1));
			 */
			latency.put(key, data);
		}
		return latency;
	}
	

	/**
	 * Method to get namespaces for Aerospike node.
	 * 
	 * @return String[] Array of Namespaces
	 */
	public String[] getNamespaces() {

		Node node = getAerospikeNodes()[0];
		String[] namespaces;
		String filter = "namespaces";
		String ns_str = "";
		if (node != null)
			ns_str = Info.request(null, node, filter);
		namespaces = ns_str.split(";");
		return namespaces;
	}


	/**
	 * Method to close all connection to Aerospike server.
	 * 
	 */
	public void closeClientConnections() {
		if (this.client != null)
			this.client.close();
	}
	
	/**
	 * New Added to handle >3.9 version
	 */
	public boolean newAsdversion(Node node) {
		logger.debug("Check ASD version is new(>3.9) or old(<3.9)");
		Map<String, String> nodeStats = null;
		if (this.client != null && node != null) {
			nodeStats = Info.request(null, node);
			String build = nodeStats.get("build");
			logger.debug("Node build: " + build);
			String[] ver = build.split("[.]");
			if (Integer.parseInt(ver[0]) > 3 || (Integer.parseInt(ver[0]) == 3 && Integer.parseInt(ver[1]) >= 9)) {
				logger.info("New ASD > 3.9");
				return true;
			} else {
				logger.info("Old ASD < 3.9");
				return false;
			}	
		} else {
			logger.debug("Client or node is down. Not able to get info");
			throw new NullPointerException("Client or node is down. Not able to get info");
		}
	}

	
	public Map<String, Map<String, String>> getThroughput(Node node) {
		logger.debug("Get node throughput");
		String oldReadReqs = "";
		String oldReadSuccess = "";
		String oldWriteReqs = "";
		String oldWriteSuccess = "";
		String newReadReqs = "";
		String newReadSuccess = "";
		String newWriteReqs = "";
		String newWriteSuccess = "";

		Map<String, String> writeTpsHistory = new HashMap<String, String>();
		Map<String, String> readTpsHistory = new HashMap<String, String>();
		Map<String, Map<String, String>> output = new HashMap<String, Map<String, String>>();
		
		String nodeName = node.getHost().name;
		if (Main.rwStatsHistory.containsKey(nodeName)) {
			if (Main.rwStatsHistory.get(nodeName).containsKey("readReqs"))
				oldReadReqs = Main.rwStatsHistory.get(nodeName).get("readReqs");

			if (Main.rwStatsHistory.get(nodeName).containsKey("readSuccess"))
				oldReadSuccess = Main.rwStatsHistory.get(nodeName).get("readSuccess");

			if (Main.rwStatsHistory.get(nodeName).containsKey("writeReqs"))
				oldWriteReqs = Main.rwStatsHistory.get(nodeName).get("writeReqs");

			if (Main.rwStatsHistory.get(nodeName).containsKey("writeReqs"))
				oldWriteSuccess = Main.rwStatsHistory.get(nodeName).get("writeReqs");
		}

		Map<String, String> readWriteInfo = getReadWriteInfoFromNodes(node);
		newReadReqs = readWriteInfo.get("readReqs");
		newReadSuccess = readWriteInfo.get("readSuccess");
		newWriteReqs = readWriteInfo.get("writeReqs");
		newWriteSuccess = readWriteInfo.get("writeReqs");

		Main.rwStatsHistory.put(nodeName, readWriteInfo);


		//Integer timestamp = Calendar.getInstance().get(Calendar.MINUTE) * 60 + Calendar.getInstance().get(Calendar.SECOND);
		Long timeStamp = System.currentTimeMillis() / 1000l;

		boolean writeCondition = newWriteReqs.isEmpty() || newWriteSuccess.isEmpty() || oldWriteReqs.isEmpty()
				|| oldWriteSuccess.isEmpty();
		boolean readCondition = newReadReqs.isEmpty() || newReadSuccess.isEmpty() || oldReadReqs.isEmpty()
				|| oldReadSuccess.isEmpty();

		if (!writeCondition) {
			Float totalWriteTps = Float.valueOf(newWriteReqs) - Float.valueOf(oldWriteReqs);
			Float successWriteTps = Float.valueOf(newWriteSuccess) - Float.valueOf(oldWriteSuccess);
			Long oldTimestamp = Long.valueOf(Main.writeTpsHistory.get(nodeName).get("timeStamp"));
			Long timeDifference = timeStamp - oldTimestamp;
			totalWriteTps = Math.abs(totalWriteTps / timeDifference);
			successWriteTps = Math.abs(successWriteTps / timeDifference);
			writeTpsHistory.put("timeStamp", Long.toString(timeStamp));
			writeTpsHistory.put("totalTps", Integer.toString(Math.round(totalWriteTps)));
			writeTpsHistory.put("successTps", Integer.toString(Math.round(successWriteTps)));
		} else {
			writeTpsHistory.put("timeStamp", Long.toString(timeStamp));
			writeTpsHistory.put("totalTps", null);
			writeTpsHistory.put("successTps", null);
		}

		if (!readCondition) {
			Float totalReadTps = Float.valueOf(newReadReqs) - Float.valueOf(oldReadReqs);
			Float successReadTps = Float.valueOf(newReadSuccess) - Float.valueOf(oldReadSuccess);
			Long oldTimestamp = Long.valueOf(Main.readTpsHistory.get(nodeName).get("timeStamp"));
			Long timeDifference = timeStamp - oldTimestamp;
			totalReadTps = Math.abs(totalReadTps / timeDifference);
			successReadTps = Math.abs(successReadTps / timeDifference);
			readTpsHistory.put("timeStamp", Long.toString(timeStamp));
			readTpsHistory.put("totalTps", Integer.toString(Math.round(totalReadTps)));
			readTpsHistory.put("successTps", Integer.toString(Math.round(successReadTps)));
		} else {
			readTpsHistory.put("timeStamp", Long.toString(timeStamp));
			readTpsHistory.put("totalTps", null);
			readTpsHistory.put("successTps", null);
		}
		Main.setReadTpsHistory(readTpsHistory, node);
		Main.setWriteTpsHistory(writeTpsHistory, node);

		output.put("reads", readTpsHistory);
		output.put("writes", writeTpsHistory);
		return output;
	}
	
	
	public Map<String, String> getReadWriteInfoFromNodes(Node node) {
		boolean newAsd = newAsdversion(node);
		logger.debug("Get Read Write Info From Nodes");
		if (newAsd == false) {
			return getReadWriteInfoFromNodeStats(node);
		
		} else {
			return getReadWriteInfoFromNamespaceStats(node);
		}	
	}
	
	
	public Map<String, String> getReadWriteInfoFromNodeStats(Node node) {
		logger.debug("getReadWriteInfoFrom NodeStats");
		Map<String, String> readWriteInfo = new HashMap<String, String>();
		Map<String, String> nodeStats = getNodeStatistics(node);
		String newReadReqs = "";
		String newReadSuccess = "";
		String newWriteReqs = "";
		String newWriteSuccess = "";
		
		if (nodeStats != null && nodeStats.containsKey("stat_read_reqs"))
			newReadReqs = nodeStats.get("stat_read_reqs");

		if (nodeStats != null && nodeStats.containsKey("stat_read_success"))
			newReadSuccess = nodeStats.get("stat_read_success");

		if (nodeStats != null && nodeStats.containsKey("stat_write_reqs"))
			newWriteReqs = nodeStats.get("stat_write_reqs");

		if (nodeStats != null && nodeStats.containsKey("stat_write_success"))
			newWriteSuccess = nodeStats.get("stat_write_success");
		
		readWriteInfo.put("readReqs", newReadReqs);
		readWriteInfo.put("readSuccess", newReadSuccess);
		readWriteInfo.put("writeReqs", newWriteReqs);
		readWriteInfo.put("writeSuccess", newWriteSuccess);

		return readWriteInfo;
		
	}
	
	
	public Map<String, String> getReadWriteInfoFromNamespaceStats(Node node) {
		logger.debug("getReadWriteInfoFrom NamespaceStats");
		Map<String, String> readWriteInfo = new HashMap<String, String>();
		String[] namespaces = this.getNamespaces();
		float newReadSuccess = 0;
		float newReadReqs = 0;
		float newWriteSuccess = 0;
		float newWriteReqs = 0;

		boolean writeCondition = false;
		boolean readCondition = false;
		
		if (namespaces.length != 0) {
			for (String namespace : namespaces) {
				Map<String, String> namespaceStats = getNamespaceStatistics(namespace, node);
				//logger.debug(namespaceStats);
				if (namespaceStats != null && namespaceStats.containsKey("client_read_success") && 
						namespaceStats.containsKey("client_read_error")) {
					
					float readSuccess = Float.parseFloat(namespaceStats.get("client_read_success"));
					float readReqs = (Float.parseFloat(namespaceStats.get("client_read_error")) + 
							Float.parseFloat(namespaceStats.get("client_read_success")));
					
					logger.debug("readSuccess: " + readSuccess);
					logger.debug("readReqs: " + readReqs);

					newReadSuccess = newReadSuccess + readSuccess;
					newReadReqs = newReadReqs + readReqs;
					
					logger.debug("newReadSuccess: " + newReadSuccess);
					logger.debug("newReadReqs: " + newReadReqs);
					readCondition = true;
				}
		
				if (namespaceStats != null && namespaceStats.containsKey("client_write_success") &&
						namespaceStats.containsKey("client_write_error")) {
					
					float writeSuccess = Float.parseFloat(namespaceStats.get("client_write_success"));
					float writeReqs = (Float.parseFloat(namespaceStats.get("client_write_error")) + 
							Float.parseFloat(namespaceStats.get("client_write_success")));
					
					logger.debug("writeSuccess: " + writeSuccess);
					logger.debug("writeReqs: " + writeReqs);
					
					newWriteSuccess = newWriteSuccess + writeSuccess;
					newWriteReqs = newWriteReqs + writeReqs;
					
					logger.debug("newWriteSuccess: " + newWriteSuccess);
					logger.debug("newWriteReqs: " + newWriteReqs);
					writeCondition = true;
				}
			}
			if (readCondition == true) {
				readWriteInfo.put("readSuccess", Float.toString(newReadSuccess));
				readWriteInfo.put("readReqs", Float.toString(newReadReqs));
			} else {
				readWriteInfo.put("readSuccess", "");
				readWriteInfo.put("readReqs", "");
			}
			if (writeCondition == true) {
				readWriteInfo.put("writeSuccess", Float.toString(newWriteSuccess));
				readWriteInfo.put("writeReqs", Float.toString(newWriteReqs));
			} else {
				readWriteInfo.put("writeSuccess", "");
				readWriteInfo.put("writeReqs", "");

			}
		}	
		return readWriteInfo;		
	}


	/**
	 * Method to extract memory stat metrics from node statistics.
	 * 
	 * @param Node
	 * @return Map<String, String> Map of memory statistics
	 */
	public Map<String, String> getMemoryStats(Node node) {
		boolean newAsd = newAsdversion(node);
		if (newAsd == false) {
			return getMemoryStatsFromNodeStats(node);
		
		} else {
			return getMemoryStatsFromNamespaceStats(node);
		}
	}

	
	public Map<String, String> getMemoryStatsFromNodeStats(Node node) {
		logger.debug("Getting node memory info");		
		Map<String, String> nodeStats = getNodeStatistics(node);
		Map<String, String> memoryStats = new HashMap<String, String>();
		try {
			memoryStats.put("used_bytes_memory", nodeStats.get("used_bytes_memory"));
		} catch (Exception exception) {
			memoryStats.put("used_bytes_memory", "n/s");
			logger.error("ERROR: ", exception);
		}
		logger.info("NodeMemoryStats");
		logger.info(memoryStats);
		return memoryStats;		
	}
	
	
	public Map<String, String> getMemoryStatsFromNamespaceStats(Node node) {
		logger.debug("Getting namespace memory info");
		String[] namespaces = this.getNamespaces();
		Map<String, String> memoryStats = new HashMap<String, String>();
		float totalUsedMemory = 0;

		/*
		 * There are few changed in 3.9 stats
		 * (global stat)used-bytes-memory -> (namespace stat)memory_used_bytes
		 * (global stat)total-bytes-memory -> (namespace config)memory-size
		 */
		if (namespaces.length != 0) {
			try {
				for (String namespace : namespaces) {
					Map<String, String> namespaceStats = getNamespaceStatistics(namespace, node);
					if (namespaceStats.get("memory_used_bytes") != null) {
						float mem = Float.parseFloat(namespaceStats.get("memory_used_bytes"));
						logger.debug("usedMemory: " + mem);
						totalUsedMemory = totalUsedMemory + mem;
						logger.debug("totalUsedMemory: " + totalUsedMemory);
					}
				}
				memoryStats.put("used_bytes_memory", Float.toString(totalUsedMemory));				
			} catch (Exception exception) {
				memoryStats.put("used_bytes_memory", "n/s");
				logger.error("ERROR: ", exception);
			}
		}
		logger.info("NamespaceMemoryStats");
		logger.info(memoryStats);
		return memoryStats;
	}

	
	/**
	 * Method to extract Disk stat metrics from node statistics.
	 * @param Node
	 * @return Map<String, String> Map of Disk statistics
	 */
	public Map<String, String> getDiskStats(Node node) {
		boolean newAsd = newAsdversion(node);
		if (newAsd == false) {
			return getDiskStatsFromNodeStats(node);
		
		} else {
			return getDiskStatsFromNamespaceStats(node);
		}
	}

	
	public Map<String, String> getDiskStatsFromNodeStats(Node node) {
		logger.debug("Getting node disk info");
		Map<String, String> nodeStats = getNodeStatistics(node);
		Map<String, String> diskStats = new HashMap<String, String>();
		try {
			diskStats.put("used_bytes_disk", nodeStats.get("used_bytes_disk"));

		} catch (Exception exception) {

			diskStats.put("used_bytes_disk", "n/s");
			logger.error("ERROR: ", exception);
		}
		logger.info("NodeDiskStats");
		logger.info(diskStats);
		return diskStats;		
	}
	
	
	public Map<String, String> getDiskStatsFromNamespaceStats(Node node) {
		logger.debug("Getting namespace disk info");
		String[] namespaces = this.getNamespaces();
		Map<String, String> diskStats = new HashMap<String, String>();
		float totalUsedDisk = 0;
		//float totalDisk = (float)0.0;
		
		/*
		 *  There are few changed in 3.9 stats
		 *  (global stat)used-bytes-disk -> (namespace stat)device_used_bytes
		 *  (global stat)total-bytes-disk -> (namespace config)device-total-bytes
		 */
		
		if (namespaces.length != 0) {
			try {
				for (String namespace : namespaces) {
					Map<String, String> namespaceStats = getNamespaceStatistics(namespace, node);
					//logger.info("namespaceStats" + namespaceStats);
					//totalUsedDisk =+ Float.parseFloat(namespaceStats.get("device_used_bytes"));
					if (namespaceStats.get("device_used_bytes") != null) {
						float disk = Float.parseFloat(namespaceStats.get("device_used_bytes"));
						logger.debug("used disk: " + disk);
						totalUsedDisk = totalUsedDisk + disk;
						logger.debug("totalUsedDisk: " + totalUsedDisk);
					}
				}
				//float freeBytes = totalDisk - totalUsedDisk;
				diskStats.put("used_bytes_disk", Float.toString(totalUsedDisk));

				
			} catch (Exception exception) {
				diskStats.put("used_bytes_disk", "n/s");
				logger.error("ERROR: ", exception);
			}
		}
		logger.info("NamespaceDiskStats");
		logger.info(diskStats);
		return diskStats;
	}	
	
}

