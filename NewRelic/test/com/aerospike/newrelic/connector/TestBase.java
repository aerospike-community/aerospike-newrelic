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

	@Before
	public void createAerospikeClient() {
		base = new Base();
		base.createAerospikeClient("127.0.0.1", 3000, null, null);
	}

	@After
	public void closeClientConnections() {
		base.closeClientConnections();
	}

	@Test
	public void testGetAerospikeClient() {
		assertNotNull(base.getAerospikeClient());
	}

	@Test
	public void testGetAerospikeNode() {
		assertNotNull(base.getAerospikeNode("127.0.0.1"));

	}

	@Test
	public void testGetAerospikeNodes() {
		assertNotNull(base.getAerospikeNodes("127.0.0.1"));
	}

	@Test
	public void testGetNodeStatistics() {
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes)
			assertNotNull(base.getNodeStatistics(node));
	}

	@Test
	public void testGetNamespaces() {
		assertNotEquals("", base.getNamespaces("127.0.0.1"));
	}

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

	@Test
	public void testGetNodeLatency() {
		Map<String, Map<String, String>> latency = new HashMap<String, Map<String, String>>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes)
			assertNotEquals(latency.size(), base.getNodeLatency(node).size());
	}

	@Test
	public void testGetThroughput() {
		Map<String, Map<String, String>> output = new HashMap<String, Map<String, String>>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			assertNotEquals(output.size(), base.getThroughput(base.getNodeStatistics(node), node).size());
		}
	}

	@Test
	public void testGetMemoryStats() {
		Map<String, String> memoryStats = new HashMap<String, String>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			assertNotEquals(memoryStats.size(), base.getMemoryStats(base.getNodeStatistics(node)).size());
		}
	}
	
	@Test
	public void testGetDiskStats() {
		Map<String, String> diskStats = new HashMap<String, String>();
		Node[] nodes = base.getAerospikeNodes("127.0.0.1");
		for (Node node : nodes) {
			assertNotEquals(diskStats.size(), base.getDiskStats(base.getNodeStatistics(node)).size());
		}
	}

}
