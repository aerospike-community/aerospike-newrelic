package com.aerospike.connector;

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
	public static boolean validNumber(String value) {
		try {
			Float.parseFloat(value);
			return true;
		} catch (Exception exception) {
			return false;
		}
	}
}
