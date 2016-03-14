/**
 * 
 * Copyright Notice ===================================== This file contains
 * proprietary information of Huawei Technologies India Pvt Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2012
 * =====================================
 * 
 */

package com.huawei.iweb.platform.logging.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Class to test FileUtil methods
 * 
 * @author k00742797
 *
 */
public class FileUtilTest_UT extends TestCase{
		
    /**
     * 
     * 
     * @throws Exception
     *
     */
	@Before
	public void setUp() throws Exception {
		  File f=new File("myfile.txt");
		  if(!f.exists()){
			  f.createNewFile();
		  }
	}
	
	/**
	 * 
	 * 
	 * @throws Exception
	 *
	 */
	@After
	public void tearDown() throws Exception {
		  File f=new File("myfile.txt");
		  if(f.exists()){
			  f.delete();
		  }
	}

	/**
	 * Test method for {@link com.huawei.iweb.platform.logging.impl.FileUtil#close(java.io.Closeable)}.
	 */
	@Test
	public void testClose() {
		try {
			FileInputStream in = new FileInputStream(new File("myfile.txt"));
			FileUtil.close(in);
			assertTrue(true);
		} catch (FileNotFoundException e) {
			assertTrue(false);
		} 
	}
}
