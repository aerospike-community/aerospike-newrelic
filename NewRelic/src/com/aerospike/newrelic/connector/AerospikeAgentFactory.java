package com.aerospike.newrelic.connector;

import static com.aerospike.newrelic.utils.Constants.*;

import java.util.ArrayList;
import java.util.Map;

import org.json.simple.JSONArray;

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
		JSONArray json_seed_list = (JSONArray) properties.get("seed_list");
		String clusterName = (String) properties.get("clusterName");
		
		ArrayList<String> seed_list = new ArrayList<String>();     
		if (json_seed_list != null) { 
		   for (int i=0; i < json_seed_list.size(); i++){
			   String host_port = json_seed_list.get(i).toString();
				if (host_port.equals(LOCALHOST_PORT) || host_port == null || EMPTY_STRING.equals(host_port)) {
					host_port = DEFAULT_HOST_PORT;
					seed_list.add(host_port);
				} else {		   
					seed_list.add(json_seed_list.get(i).toString());
				}
		   } 
		}
		/**
		 * Use pre-defined defaults to simplify configuration
		 */

		if (user == null || user.equalsIgnoreCase("") || user.equalsIgnoreCase("n/a") || password == null || password.equalsIgnoreCase("")
				|| password.equalsIgnoreCase("n/a")) {
			user = password = null;
		}

		if (clusterName == null || EMPTY_STRING.equals(clusterName)) {
			clusterName = CLUSTER_FALLBACK_NAME;
		}
		
		/* creating and returning the AerospikeAgent */
		return new AerospikeAgent(seed_list, user, password, clusterName);
	}
}
