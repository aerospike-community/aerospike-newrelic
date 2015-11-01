package com.aerospike.newrelic.connector;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.cluster.Node;
import com.newrelic.metrics.publish.Runner;
import com.newrelic.metrics.publish.configuration.ConfigurationException;
import com.newrelic.metrics.publish.util.Logger;

/**
 * Main class for Aerospike Agent
 * 
 * @author Aniruddha Atre
 */
public class Main {

	public static Map<String, Map<String, String>> readTpsHistory = new HashMap<String, Map<String, String>>();
	public static Map<String, Map<String, String>> writeTpsHistory = new HashMap<String, Map<String, String>>();
	public static Map<String, Map<String, String>> statsHistory = new HashMap<String, Map<String, String>>();

	private static final Logger logger = Logger.getLogger(Main.class);

	/**
	 * Getter method for read throughput history
	 * 
	 * @return Map<String, Map<String, String>> the read throughput history
	 */
	public static Map<String, Map<String, String>> getReadTpsHistory() {
		return readTpsHistory;
	}

	/**
	 * Getter method for write throughput history
	 * 
	 * @return Map<String, Map<String, String>> the write throughput history
	 */
	public static Map<String, Map<String, String>> getWriteTpsHistory() {
		return writeTpsHistory;
	}

	/**
	 * Getter method for node statistics
	 * 
	 * @return Map<String, Map<String, String>> the statistics
	 */
	public static Map<String, Map<String, String>> getStatistics() {
		return statsHistory;
	}

	/**
	 * Setter method for read throughput history
	 * 
	 * @param Map<String,
	 *            String> read throughput data
	 * @param node
	 *            Aerospike node
	 */
	public static void setReadTpsHistory(Map<String, String> tpsData, Node node) {
		readTpsHistory.put(node.getHost().name, tpsData);
		for (Map.Entry<String, String> entry : tpsData.entrySet()) {
			boolean condition = entry.getKey().equalsIgnoreCase("x") || entry.getKey().equalsIgnoreCase("y")
					|| entry.getKey().equalsIgnoreCase("secondary");
			if (condition)
				readTpsHistory.get(node.getHost().name).put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Setter method for write throughput history
	 * 
	 * @param Map<String,
	 *            String> write throughput data
	 * @param node
	 *            Aerospike node
	 */
	public static void setWriteTpsHistory(Map<String, String> tpsData, Node node) {
		writeTpsHistory.put(node.getHost().name, tpsData);
		for (Map.Entry<String, String> entry : tpsData.entrySet()) {
			boolean condition = entry.getKey().equalsIgnoreCase("x") || entry.getKey().equalsIgnoreCase("y")
					|| entry.getKey().equalsIgnoreCase("secondary");
			if (condition)
				writeTpsHistory.get(node.getHost().name).put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Setter method for node statistics
	 * 
	 * @param Map<String,
	 *            Map<String, String>> statistics
	 */
	public static void setStatistcs(Map<String, Map<String, String>> stats) {
		statsHistory = stats;
	}

	/**
	 * Entry point main() method
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Runner runner = new Runner();
			runner.add(new AerospikeAgentFactory());
			runner.setupAndRun(); // Never Returns
		} catch (ConfigurationException configurationException) {
			logger.error("ERROR: ", configurationException);
			System.exit(-1);
		}
	}
}