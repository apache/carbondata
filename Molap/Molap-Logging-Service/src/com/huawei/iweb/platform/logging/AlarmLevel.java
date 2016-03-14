/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 *
 */
package com.huawei.iweb.platform.logging;

import org.apache.log4j.Level;

/**
 * 
 * Custom Level logging service
 * 
 * @author A00900294
 * 
 */
public class AlarmLevel extends Level
{

    /**
     * Aug 29, 2012
     */
    private static final long serialVersionUID = 4105910328789997397L;

    /**
     * This level is used to encrypt log message and to avoid writing into DB
     */
    public static final AlarmLevel ALARM = new AlarmLevel(60000, "ALARM", 0);

    /**
     * Constructor
     * 
     * @param level
     *            log level
     * @param levelStr
     *            log level string
     * @param syslogEquivalent
     *            syslogEquivalent
     * 
     */
    protected AlarmLevel(int level, String levelStr, int syslogEquivalent)
    {
        super(level, levelStr, syslogEquivalent);
    }

    /**
     * Returns custom level for secure type log message
     * 
     * @param val
     *            value
     * @param defaultLevel
     *            level
     * @return custom level
     */
    public static AlarmLevel toLevel(int val, Level defaultLevel)
    {
        return ALARM;
    }

    /**
     * Returns custom level for secure type log message
     * 
     * @param sArg
     *            sArg
     * @param defaultLevel
     *            level
     * @return custom level
     */
    public static AlarmLevel toLevel(String sArg, Level defaultLevel)
    {
        return ALARM;
    }
}
