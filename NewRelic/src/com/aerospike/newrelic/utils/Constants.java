package com.aerospike.newrelic.utils;

import java.util.regex.Pattern;

public class Constants {

	public static final String COMMA = ",";
	public static final String SLASH = "/";
	public static final String SPACE = " ";
	public static final String EMPTY_STRING = "";
	public static final String UNDERSCORE = "_";

	public static final String SEPARATOR = "/";
	// Only integers and floats are valid metric values
	public static final Pattern VALID_METRIC_PATTERN = Pattern.compile("(-)?(\\.)?\\d+(\\.\\d+)?");
	public static final Pattern SPACE_PATTERN = Pattern.compile(" ");

	public static final String ON = "ON";
	public static final String OFF = "OFF";
	public static final String NULL = "NULL";
	public static final String TRUE = "true";
	public static final String FALSE = "false";

	public static final String ONE = "1";
	public static final String ZERO = "0";

	public static final String DEFAULT_PLUGIN_NAME = "Aerospike";
	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final String LOCALHOST = "localhost";
	public static final String DEFAULT_PORT = "3000";
	public static final String CLUSTER_FALLBACK_NAME = "clusterName";

	public static final String SUMMARY = "summary";
	public static final String NODE_STATS = "nodeStats";
	public static final String THROUGHPUT_STATS = "throughputStats";
	public static final String LATENCY_STATS = "latencyStats";
	public static final String NAMESPACE_STATS = "namespaceStats";
	public static final String LATENCY = "latency";
	public static final String READS = "reads";
	public static final String WRITES = "writes";

	public static final String METRIC_BASE_NAME = "aerospike";

	public static final String[] LATENCY_CATEGORY = { "reads", "writes_master", "query", "udf", "proxy" };
	public static final String[] LATENCY_BUCKETS = { "LT 1ms", "GT 1ms to LT 8ms", "GT 8ms to LT 64ms", "GT 64ms" };

}
