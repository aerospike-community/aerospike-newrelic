package com.aerospike.connector;

import java.util.Map;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.AgentFactory;
import com.newrelic.metrics.publish.configuration.ConfigurationException;

/**
 * AgentFactory for Aerospike. The name of the Aerospike node, node
 * address(host:port) and login credentials.
 * 
 * @author Aniruddha Atre
 */
public class AerospikeAgentFactory extends AgentFactory {

	/**
	 * Method to create Aerospike agent
	 * 
	 * @param Map<String,
	 *            String> properties of Aerospike node.
	 * @return Aerospike Agent
	 */
	@Override
	public Agent createConfiguredAgent(Map<String, Object> properties) throws ConfigurationException {

		String user = (String) properties.get("user");
		String password = (String) properties.get("password");
		String host = (String) properties.get("host");
		String port = (String) properties.get("port");
		String name = (String) properties.get("clusterName");

		return new AerospikeAgent(host, port, user, password, name);
	}
}
