package com.aerospike.newrelic.connector;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.cluster.Node;

public class TestBase {
	private Base base;

	/**
	 * method to be executed before every unit test case
	 */
	@Before
	public void createAerospikeClient() {
		base = new Base();
		base.createAerospikeClient("127.0.0.1", 3000, null, null);
	}

	/**
	 * method to be executed after every unit test case
	 */
	@After
	public void closeClientConnections() {
		base.closeClientConnections();
	}

	/**
	 * method to unit test getAerospikeClient() of Base class
	 */
	@Test
	public void testGetAerospikeClient() {
		assertNotNull(base.getAerospikeClient());
	}

	/**
	 * method to unit test getAerospikeNode() of Base class
	 */
	@Test
	public void testGetAerospikeNode() {
		assertNotNull(base.getAerospikeNode("127.0.0.1"));

	}

	/**
	 * method to unit test getAerospikeNodes() of Base class
	 */
	@Test
	public void testGetAerospikeNodes() {
		assertNotNull(base.getAerospikeNodes("127.0.0.1"));
	}

	/**
	 * method to unit test getNodeStatistics() of Base class
	 */
	@Test
	public void testGetNodeStatistics() {
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes)
			assertNotNull(base.getNodeStatistics(node));
	}

	/**
	 * method to unit test getNamespaces() of Base class
	 */
	@Test
	public void testGetNamespaces() {
		assertNotEquals("", base.getNamespaces("127.0.0.1"));
	}

	/**
	 * method to unit test getNamespaceStatistics() of Base class
	 */
	@Test
	public void testGetNamespaceStatistics() {
		String[] namespaces = base.getNamespaces("127.0.0.1");
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (String namespace : namespaces) {
			for (Node node : nodes) {
				assertNotNull(base.getNamespaceStatistics(namespace, node));
			}
		}
	}

	/**
	 * method to unit test getNodeLatency() of Base class
	 */
	@Test
	public void testGetNodeLatency() {
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			if (base.getNodeLatency(node).size() > 0) {
				assertEquals(true, base.getNodeLatency(node).keySet().contains("reads"));
				assertEquals(true, base.getNodeLatency(node).keySet().contains("writes_master"));
				assertEquals(true, base.getNodeLatency(node).keySet().contains("proxy"));
				assertEquals(true, base.getNodeLatency(node).keySet().contains("udf"));
				assertEquals(true, base.getNodeLatency(node).keySet().contains("query"));
			}
		}
	}

	/**
	 * method to unit test getThroughput() of Base class
	 */
	@Test
	public void testGetThroughput() {
		Map<String, Map<String, String>> output = new HashMap<String, Map<String, String>>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			assertNotEquals(output.size(), base.getThroughput(base.getNodeStatistics(node), node).size());
		}
	}

	/**
	 * method to unit test getMemoryStats() of Base class
	 */
	@Test
	public void testGetMemoryStats() {
		Map<String, String> memoryStats = new HashMap<String, String>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			assertNotEquals(memoryStats.size(), base.getMemoryStats(base.getNodeStatistics(node)).size());
		}
	}

	/**
	 * method to unit test getDiskStats() of Base class
	 */
	@Test
	public void testGetDiskStats() {
		Map<String, String> diskStats = new HashMap<String, String>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			assertNotEquals(diskStats.size(), base.getDiskStats(base.getNodeStatistics(node)).size());
		}
	}

}
