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

import com.huawei.iweb.platform.logging.impl.StandardLogService;

/**
 * Class to test LogServiceFactory methods
 * 
 * @author k00742797
 *
 */
public class LogServiceFactoryTest_UT extends TestCase{

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetLogService() {
            LogService logger = LogServiceFactory.getLogService("sampleclass");
            assertTrue(logger instanceof StandardLogService);
	}

}
