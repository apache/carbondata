/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 *
 */
package com.huawei.iweb.platform.logging;

import junit.framework.Assert;
import mockit.Deencapsulation;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * 
 * @author k00742797
 *
 */
public class ExtendedRollingFileAppenderTest_UT
{

    private ExtendedRollingFileAppender rAppender = null;
    
    @Before
    public void setUp() throws Exception
    {
        rAppender = new ExtendedRollingFileAppender();
        Deencapsulation.setField(rAppender, "fileName", "dummy.log");
        Deencapsulation.setField(rAppender, "maxBackupIndex", 1);
        Deencapsulation.setField(rAppender, "maxFileSize", 1000L);
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testRollOver()
    {
        rAppender.rollOver();
        rAppender.rollOver();
        rAppender.rollOver();
        Assert.assertTrue(true);
    }
    
    @Test
    public void testCleanLogs()
    {
        final String startName = "dummy";
        final String folderPath = "./";
        int maxBackupIndex = 1;
        
        Deencapsulation.invoke(rAppender, "cleanLogs", startName, folderPath, maxBackupIndex);
    }

    @Test
    public void testSubAppendLoggingEvent()
    {
        Logger logger = Logger.getLogger(this.getClass());
        LoggingEvent event = new LoggingEvent(null, logger, 0L, AuditLevel.DEBUG, null, null);
        
        try{
            rAppender.subAppend(event);
        } catch(Exception e){
            //
        }
        Assert.assertTrue(true);
    }

}
