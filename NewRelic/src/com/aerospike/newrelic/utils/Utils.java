package com.aerospike.newrelic.utils;

import static com.aerospike.newrelic.utils.Constants.*;

/**
 * Utils class having utility methods.
 * 
 * @author Aniruddha Atre
 *
 */
public class Utils {

	/**
	 * Method to check if metric is a valid number or not.
	 * 
	 * @param value
	 *            A metric to be published
	 * @return Boolean True if valid number else false
	 */
	public static boolean isValidNumber(String value) {
		try {
			Float.parseFloat(value);
			return true;
		} catch (Exception exception) {
			return false;
		}
	}

	/**
	 * Method to check if stats from aerospike have value true/false
	 * 
	 * @param value
	 * @return 1/0 for true/false or return value as it is
	 */
	public static String transformStringMetric(String value) {
		if (value.equalsIgnoreCase(TRUE))
			return ONE;
		else if (value.equalsIgnoreCase(FALSE))
			return ZERO;
		return value;
	}

	/**
	 * Check if the value is a valid New Relic Metric value
	 * 
	 * @param val
	 *            String to validate
	 * @return TRUE if string is a numeric supported by New Relic
	 */
	public static boolean validMetricValue(String val) {
		if (val == null || EMPTY_STRING.equals(val)) {
			return false;
		}
		if (VALID_METRIC_PATTERN.matcher(val).matches()) {
			return true;
		}
		return false;
	}

}
