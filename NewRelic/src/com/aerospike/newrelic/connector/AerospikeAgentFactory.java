package com.aerospike.newrelic.connector;

import static com.aerospike.newrelic.utils.Constants.*;
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
		String clusterName = (String) properties.get("clusterName");

		/**
		 * Use pre-defined defaults to simplify configuration
		 */
		if (host == null || EMPTY_STRING.equals(host)) {
			host = DEFAULT_HOST;
		}

		if (port == null || EMPTY_STRING.equals(port)) {
			port = DEFAULT_PORT;
		}

		if (user.equalsIgnoreCase("") || user.equalsIgnoreCase("n/a") || password.equalsIgnoreCase("")
				|| password.equalsIgnoreCase("n/a")) {
			user = password = null;
		}

		if (clusterName == null || EMPTY_STRING.equals(clusterName)) {
			clusterName = CLUSTER_FALLBACK_NAME;
		}
		
		//creating and returning the AerospikeAgent
		return new AerospikeAgent(host, port, user, password, clusterName);
	}
}
