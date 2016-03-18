/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
public class AuditLevel extends Level
{

    /**
     * Aug 29, 2012
     */
    private static final long serialVersionUID = -209614723183147373L;

    /**
     * AUDIT
     */
    public static final AuditLevel AUDIT = new AuditLevel(55000, "AUDIT", 0);

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
    protected AuditLevel(int level, String levelStr, int syslogEquivalent)
    {
        super(level, levelStr, syslogEquivalent);
    }

    /**
     * Returns custom level for debug type log message
     * 
     * @param val
     *            value
     * @param defaultLevel
     *            level
     * @return custom level
     */
    public static AuditLevel toLevel(int val, Level defaultLevel)
    {
        return AUDIT;
    }

    /**
     * Returns custom level for debug type log message
     * 
     * @param sArg
     *            sArg
     * @param defaultLevel
     *            level
     * @return custom level
     */
    public static AuditLevel toLevel(String sArg, Level defaultLevel)
    {
        return AUDIT;
    }
}
