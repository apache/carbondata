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

import org.apache.log4j.spi.LoggingEvent;

/**
 * 
 * Copied form log4j and modified for renaming files and restriction only for
 * audit logging
 * 
 * @author k00742797
 * 
 */
public class AuditExtendedRollingFileAppender extends ExtendedRollingFileAppender
{

    /**
     * Call RollingFileAppender method to append the log...
     * 
     * @see org.apache.log4j.RollingFileAppender#subAppend(org.apache.log4j.spi.LoggingEvent)
     * 
     */
    protected void subAppend(LoggingEvent event)
    {
        if(event.getLevel().toInt() == AuditLevel.AUDIT.toInt())
        {
            currentLevel = AuditLevel.AUDIT.toInt();
            super.subAppend(event);
        }
    }
}
