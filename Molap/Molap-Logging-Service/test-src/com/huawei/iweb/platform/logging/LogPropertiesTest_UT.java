/**
 * 
 * Copyright Notice ===================================== This file contains
 * proprietary information of Huawei Technologies India Pvt Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2012
 * =====================================
 * 
 */
package com.huawei.iweb.platform.logging;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Class to test LogProperties methods
 * 
 * @author k00742797
 *
 */
public class LogPropertiesTest_UT extends TestCase{

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetKey() {
        String result = LogProperties.FEATURE.getKey();
        assertEquals("result", "FEATURE", result);
	}
	
	@Test
	public void testValues() {
        LogProperties[] result = LogProperties.values();
        assertEquals("result.length", 8, result.length);
        assertEquals("result[0]", LogProperties.USER_NAME, result[0]);
	}	

	@Test
	public void testToString() {
        String result = LogProperties.USER_NAME.toString();
        assertEquals("result", "USER_NAME", result);
	}

}
