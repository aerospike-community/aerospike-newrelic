package com.aerospike.connector;


import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.newrelic.metrics.publish.Runner;
import com.newrelic.metrics.publish.configuration.ConfigurationException;

/**
 * Main class for Aerospike Agent
 * @author Aniruddha Atre
 */
public class Main {

	public static Map<String, String> readTpsHistory = new HashMap<String, String>();
	public static Map<String, String> writeTpsHistory = new HashMap<String, String>();
	public static Map<String, String> statsHistory = new HashMap<String, String>();
	
	/**
     * Constructor for Main.
     */
	public Main() {
		
		Integer ts = (int) Calendar.getInstance().get(Calendar.MILLISECOND);
		readTpsHistory.put("x", Integer.toString(ts));
		readTpsHistory.put("y", null);
		readTpsHistory.put("secondary", null);
		
		writeTpsHistory.put("x", Integer.toString(ts));
		writeTpsHistory.put("y", null);
		writeTpsHistory.put("secondary", null);
	}
	
	 /**
     * Getter method for read throughput history
     * @return Map<String, String> the read throughput history
     */
	public static Map<String, String> getReadTpsHistory(){
		return readTpsHistory;
	}
	
	/**
     * Getter method for write throughput history
     * @return Map<String, String> the write throughput history
     */
	public static Map<String, String> getWriteTpsHistory(){
		return writeTpsHistory;
	}
	
	/**
     * Getter method for node statistics
     * @return Map<String, String> the statistics
     */
	public static Map<String, String> getStatistics(){
		return statsHistory;
	}
	
	/**
     * Setter method for read throughput history
     * @param Map<String, String> read throughput data
     */
	public static void setReadTpsHistory(Map<String, String> tpsData){
		for(Map.Entry<String, String> entry: tpsData.entrySet()){
			boolean condition = entry.getKey().equalsIgnoreCase("x") || entry.getKey().equalsIgnoreCase("y") || entry.getKey().equalsIgnoreCase("secondary");
			if(condition)
				readTpsHistory.put(entry.getKey(), entry.getValue());
		}
	}
	
	/**
     * Setter method for write throughput history
     * @param Map<String, String> write throughput data
     */
	public static void setWriteTpsHistory(Map<String, String> tpsData){
		for(Map.Entry<String, String> entry: tpsData.entrySet()){
			boolean condition = entry.getKey().equalsIgnoreCase("x") || entry.getKey().equalsIgnoreCase("y") || entry.getKey().equalsIgnoreCase("secondary");
			if(condition)
				writeTpsHistory.put(entry.getKey(), entry.getValue());
		}
	}
	
	/**
     * Setter method for node statistics
     * @param Map<String, String> statistics
     */
	public static void setStatistcs(Map<String, String> stats){
		statsHistory = stats;
	}
	
	public static void main(String [] args){
		try{
			Runner runner = new Runner();
			runner.add(new AerospikeAgentFactory());
			runner.setupAndRun(); // Never Returns
		} catch(ConfigurationException e){
			 System.err.println("ERROR: " + e.getMessage());
	         System.exit(-1);
		}
	}
}