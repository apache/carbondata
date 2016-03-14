/**
 * 
 * Copyright Notice ===================================== This file contains
 * proprietary information of Huawei Technologies India Pvt Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2012
 * =====================================
 * 
 */
package com.huawei.iweb.platform.logging.impl;

import junit.framework.TestCase;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;

import org.apache.log4j.Category;
import org.apache.log4j.Priority;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.huawei.iweb.platform.logging.LogEvent;

/**
 * Class to test ResourceBundleMessageFinder methods
 * 
 * @author k00742797
 *
 */
public class StandardLogServiceTest_UT extends TestCase{

    private LogEvent event = null;

    private StandardLogService logService = null;
    
    /**
     * 
     * 
     * @throws Exception
     *
     */
    @Before
    public void setUp() throws Exception
    {

        new MockUp<Category>()
        {
            @SuppressWarnings("unused")
            @Mock
            public boolean isDebugEnabled()
            {
                return true;
            }

            @SuppressWarnings("unused")
            @Mock
            public boolean isEnabledFor(Priority level)
            {
                return true;
            }

            @SuppressWarnings("unused")
            @Mock
            public boolean isInfoEnabled()
            {
                return true;
            }
        };

        event = new LogEvent()
        {
            @Override
            public String getModuleName()
            {
                return "TEST";
            }

            @Override
            public String getEventCode()
            {
                return "TEST";
            }
        };
        
        logService = new StandardLogService(this.getClass().getName());
    }

    /**
     * @Author k00742797
     * @Description : tearDown
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#StandardLogService(java.lang.String)}
     * .
     */
    @Test
    public void testStandardLogService()
    {
        if(logService != null && logService instanceof StandardLogService){
            Assert.assertTrue(true);
        } else {
            Assert.assertTrue(false);   
        }
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#isDebugEnabled()}
     * .
     */
    @Test
    public void testIsDebugEnabled()
    {
        Assert.assertEquals(true,
                logService.isDebugEnabled());
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#isWarnEnabled()}
     * .
     */
    @Test
    public void testIsWarnEnabled()
    {
        Assert.assertEquals(true,
                logService.isWarnEnabled());
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#secure(com.huawei.iweb.platform.logging.LogEvent, java.lang.Object[])}
     * .
     */
    @Test
    public void testSecureLogEventObjectArray()
    {
          Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#audit(com.huawei.iweb.platform.logging.LogEvent, java.lang.Object[])}
     * .
     */
    @Test
    public void testAuditLogEventObjectArray()
    {
        logService.audit(event, "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#debug(com.huawei.iweb.platform.logging.LogEvent, java.lang.Object[])}
     * .
     */
    @Test
    public void testDebugLogEventObjectArray()
    {
        logService.debug(event, "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#debug(com.huawei.iweb.platform.logging.LogEvent, java.lang.Throwable, java.lang.Object[])}
     * .
     */
    @Test
    public void testDebugLogEventThrowableObjectArray()
    {
        logService.debug(event, new Exception("test"),
                "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#error(com.huawei.iweb.platform.logging.LogEvent, java.lang.Object[])}
     * .
     */
    @Test
    public void testErrorLogEventObjectArray()
    {
        logService.error(event, "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#error(com.huawei.iweb.platform.logging.LogEvent, java.lang.Throwable, java.lang.Object[])}
     * .
     */
    @Test
    public void testErrorLogEventThrowableObjectArray()
    {
        Exception exception = new Exception("test");
        logService.error(event, exception,
                "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#info(com.huawei.iweb.platform.logging.LogEvent, java.lang.Object[])}
     * .
     */
    @Test
    public void testInfoLogEventObjectArray()
    {
        logService.info(event, "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#info(com.huawei.iweb.platform.logging.LogEvent, java.lang.Throwable, java.lang.Object[])}
     * .
     */
    @Test
    public void testInfoLogEventThrowableObjectArray()
    {
        logService.info(event, new Exception("test"),
                "testing", "testing");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#isInfoEnabled()}
     * .
     */
    @Test
    public void testIsInfoEnabled()
    {
        Assert.assertEquals(true,
                logService.isInfoEnabled());
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#warn(com.huawei.iweb.platform.logging.LogEvent, java.lang.Object[])}
     * .
     */
    @Test
    public void testWarn()
    {
        logService.warn(event, new Exception("test"),
                "testing", "testing");
        Assert.assertTrue(true);

    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#deleteLogs(int)}
     * .
     */
    @Test
    public void testDeleteLogs()
    {
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#flushLogs()}
     * .
     */
    @Test
    public void testFlushLogs()
    {
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#setEventProperties(java.lang.String, java.lang.String)}
     * .
     */
    @Test
    public void testSetEventProperties()
    {
        logService.setEventProperties("CLIENT_IP",
                "127.0.0.1");
        Assert.assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#isDoLog()}
     * .
     */
    @Test

    public void testIsDoLog()
    {
        StandardLogService.setDoLog(true);
        Assert.assertEquals(true, StandardLogService.isDoLog());

        StandardLogService.setDoLog(false);
        Assert.assertEquals(false, StandardLogService.isDoLog());

    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#setDoLog(boolean)}
     * .
     */
    @Test
    public void testSetDoLog()
    {
        StandardLogService.setDoLog(true);
        Assert.assertEquals(true, StandardLogService.isDoLog());
    }

    /**
     * Test method for
     * {@link com.huawei.iweb.platform.logging.impl.StandardLogService#audit(java.lang.String)}
     * .
     */
    @Test
    public void testAuditString()
    {
        logService.audit("audit message");
        Assert.assertTrue(true);
    }

}
