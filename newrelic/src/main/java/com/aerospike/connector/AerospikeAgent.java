package com.aerospike.connector;

import java.util.Map;


import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.configuration.ConfigurationException;
import com.aerospike.connector.Base;

/**
 * Agent for Aerospike. 
 * This agent will log Aerospike statistics, namespace statistics, latency and throughput metrics for an Aerospike node.  
 * @author Aniruddha Atre
 */
public class AerospikeAgent extends Agent{

	private static final String GUID = "com.aerospike.connector";
    private static final String VERSION = "1.0.0";

    private String user;
    private String password;
    private String host;
    private Integer port;
    private String name;
	
    /**
     * Constructor for Aerospike Agent
     * @param Aerospike node details (host, port, user, password, node name)
     * @throws ConfigurationException if error reading configuration parameters.
	 * */
	public AerospikeAgent(String host, String port, String user, String password, String name)throws ConfigurationException {
		super(GUID, VERSION);
		try {
			
			this.host = host;
			this.port = Integer.parseInt(port);
			if(user.equalsIgnoreCase("n/a"))
				this.user = this.password = null;
			else{
				this.user = user;
				this.password = password;
			}
			this.name = name.replaceAll("\\s", "");
        } catch (Exception e) {
            throw new ConfigurationException("Error reading configuration parameters...", e);
        }
	}

	/**
     * Method to return agent name.
     * @return String Aerospike Agent Name
     */
	@Override
	public String getAgentName() {
		return "Aerospike";
	}
	
	/**
	 * Method to check if metric is a valid number or not.
	 * @param value A metric to be published
	 * @return Boolean True if valid number else false
	 */
	public boolean validNumber(String value){
		try{
			Float.parseFloat(value);
			return true;
		}
		catch(Exception e){
			return false;
		}
	}
	
	/**
	 * Method to fetch node statistics from base and report to new relic.
	 * @param obj Base object
	 * @return Map<String, String> A map of node statistics
	 */
	public Map<String, String>  reportNodeStatistics(Base obj){
		
		Map<String, String> nodeStats = obj.getNodeStatistics(this.host, this.port, this.user, this.password);
		
		if(nodeStats != null){
			String namePrefix = "aerospike/" + this.name + "/";
			for(Map.Entry<String, String> stat : nodeStats.entrySet()){
				reportMetric(namePrefix + stat.getKey(), "", Float.parseFloat(stat.getValue()));
				System.out.println(namePrefix + stat.getKey() + stat.getValue());
			}
		}
		
		Map<String, String> memoryStats = obj.getMemoryStats(nodeStats);
		Map<String, String> diskStats = obj.getDiskStats(nodeStats);
		if(validNumber(diskStats.get("free-bytes-disk"))){
			reportMetric("aerospike/" + this.name + "/disk_usage_free", "", Float.parseFloat(diskStats.get("free-bytes-disk")));
		}
		if(validNumber(diskStats.get("total-bytes-disk"))){
			reportMetric("aerospike/" + this.name + "/disk_usage_total", "", Float.parseFloat(diskStats.get("total-bytes-disk")));
		}
		if(validNumber(memoryStats.get("free-bytes-memory"))){
			reportMetric("aerospike/" + this.name + "/memory_usage_free", "", Float.parseFloat(memoryStats.get("free-bytes-memory")));
		}
		if(validNumber(memoryStats.get("total-bytes-memory"))){
			reportMetric("aerospike/" + this.name + "/memory_usage_total", "", Float.parseFloat(memoryStats.get("total-bytes-memory")));
		}
		reportThroughput(obj, nodeStats);
		return nodeStats;
	}
	
	/**
	 * A method to report node latency for reads, writes_master, proxy, udf and query.
	 * @param obj Base object
	 */
	public void reportNodeLatency(Base obj){
		
		Map<String, Map<String, String>> latency = obj.getNodeLatency(this.host, this.port, this.user, this.password);
		for(Map.Entry<String, Map<String, String>>entry: latency.entrySet()){
			String key = entry.getKey();
			for(Map.Entry<String, String>dataEntry: entry.getValue().entrySet()){
				reportMetric("aerospike/" + this.name + "/latency/" + key + "/" + dataEntry.getKey() + "/value", "", Float.parseFloat(dataEntry.getValue().split(";")[0]));
				reportMetric("aerospike/" + this.name + "/latency/" + key + "/" + dataEntry.getKey() + "/pct", "", Float.parseFloat(dataEntry.getValue().split(";")[1]));
			}
		}
	}
	
	/**
	 * Method to report node throughput.
	 * @param obj Base object
	 * @param nodeStats Map of node statistics
	 */
	public void reportThroughput(Base obj, Map<String, String> nodeStats){
		
		Map<String, Map<String,String>> tps = obj.getThroughput(nodeStats);
		Map<String, String> readTps = tps.get("reads");
		Map<String, String> writeTps = tps.get("writes");
		if(readTps!= null && readTps.get("y") == null)
			reportMetric("aerospike/" + this.name + "/throughput/reads/success", "", 0);
		else
			reportMetric("aerospike/" + this.name + "/throughput/reads/success", "", Integer.valueOf(readTps.get("y")));
		if(readTps.get("secondary") == null)
			reportMetric("aerospike/" + this.name + "/throughput/reads/total", "", 0);
		else
			reportMetric("aerospike/" + this.name + "/throughput/reads/total", "", Integer.valueOf(readTps.get("secondary")));

		if(writeTps.get("y") == null)
			reportMetric("aerospike/" + this.name + "/throughput/writes/success", "", 0);
		else
			reportMetric("aerospike/" + this.name + "/throughput/writes/success", "", Integer.valueOf(writeTps.get("y")));
		if(writeTps.get("secondary") == null)
			reportMetric("aerospike/" + this.name + "/throughput/writes/total", "", 0);
		else
			reportMetric("aerospike/" + this.name + "/throughput/writes/total", "", Integer.valueOf(writeTps.get("secondary")));
	}
	
	/**
	 * Method to report namespace statistics.
	 * @param obj Base object
	 * @param namespace namespace name to fetch the statistics
	 */
	public void reportNamespaceStats(Base obj, String namespace){
		
		Map<String, String> namespaceStats = obj.getNamespaceStatistics(namespace, this.host, this.port, this.user, this.password);
		if(namespaceStats != null){
			String nsPrefix = "aerospike/" + this.name + "/" + namespace + "/";
			for(Map.Entry<String, String> stat: namespaceStats.entrySet()){
				reportMetric(nsPrefix + stat.getKey(), "", Float.parseFloat(stat.getValue()));
			}
		}
	}
	
	/**
	 * A method to submit Aerospike metrics to New Relic.
	 */
	@Override
	public void pollCycle() {
		
		Base obj = new Base();
		
		obj.createAerospikeClient(this.host, this.port, this.user, this.password);
		Map<String, String> nodeStats = reportNodeStatistics(obj);
		reportNodeLatency(obj);
		Main.setStatistcs(nodeStats);
		
		String [] namespaces = obj.getNamespaces(this.host, this.port, this.user, this.password);
		
		if(namespaces.length != 0){
			for(String ns: namespaces){
				reportNamespaceStats(obj, ns);
			}
		}
	}
}
