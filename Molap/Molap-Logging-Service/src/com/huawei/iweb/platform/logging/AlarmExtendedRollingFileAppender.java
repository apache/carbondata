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
 * 
 * Only for alarm logs
 * 
 * @author k00742797
 * 
 */
public class AlarmExtendedRollingFileAppender extends
        ExtendedRollingFileAppender
{

    /**
     * 
     * 
     * @see com.huawei.iweb.platform.logging.ExtendedRollingFileAppender#subAppend(org.apache.log4j.spi.LoggingEvent)
     * 
     */
    protected void subAppend(LoggingEvent event)
    {
        if(event.getLevel().toInt() == AlarmLevel.ALARM.toInt())
        {
            currentLevel = AlarmLevel.ALARM.toInt();
            super.subAppend(event);
        }
    }
}
