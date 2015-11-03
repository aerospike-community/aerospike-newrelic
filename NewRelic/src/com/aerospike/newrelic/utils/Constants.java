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
	public static final String NODE = "node";
	public static final String THROUGHPUT = "throughput";
	public static final String LATENCY = "latency";
	public static final String NAMESPACE = "namespace";
	public static final String READS = "reads";
	public static final String WRITES = "writes";

	public static final String METRIC_BASE_NAME = "aerospike";

}
