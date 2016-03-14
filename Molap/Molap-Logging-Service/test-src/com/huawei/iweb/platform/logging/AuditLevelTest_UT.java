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

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Class to test AuditLevel methods
 * 
 * @author k00742797
 *
 */
public class AuditLevelTest_UT extends TestCase{

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAuditLevel() {
		assertEquals(AuditLevel.AUDIT.toInt(), 55000);
	}

	@Test
	public void testToLevelIntLevel() {
		assertSame(AuditLevel.AUDIT, AuditLevel.toLevel(55000, Level.DEBUG));
	}

	@Test
	public void testToLevelStringLevel() {
		assertSame(AuditLevel.AUDIT, AuditLevel.toLevel("AUDIT", Level.DEBUG));
	}

}
