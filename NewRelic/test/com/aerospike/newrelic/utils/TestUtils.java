package com.aerospike.newrelic.utils;

import static org.junit.Assert.*;
import static com.aerospike.newrelic.utils.Constants.*;

import org.junit.Test;

public class TestUtils {
	@Test
	public void testIsValidNumber() {
		assertEquals(true, Utils.isValidNumber("1.0"));

		assertEquals(false, Utils.isValidNumber("true"));
	}

	@Test
	public void testTransformStringMetric() {
		assertEquals(ONE, Utils.transformStringMetric("true"));

		assertEquals(ZERO, Utils.transformStringMetric("false"));

		assertEquals("10.0", Utils.transformStringMetric("10.0"));
	}

	@Test
	public void testValidMetricValue() {
		assertEquals(false, Utils.validMetricValue(null));

		assertEquals(false, Utils.validMetricValue(""));

		assertEquals(true, Utils.validMetricValue("10.0"));

		assertEquals(false, Utils.validMetricValue("false"));
	}
}
