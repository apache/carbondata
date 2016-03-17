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
 * Project Name NSE V300R006C00B123
 * Module Name : 
 * Author C00900810
 * Created Date :Jan 3, 2013 7:22:00 PM
 * FileName : SecureLevel.java
 * Class Description :
 * Version 1.0
 * ====================Copyright Notice =======================
 *  This file contains proprietary information of Huawei Technologies Co. Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2010
 * =========================================================
 */
package com.huawei.iweb.platform.logging;

import org.apache.log4j.Level;

public class SecureLevel  extends Level{

	/**
	 * C00900810
	 * Jan 3, 2013
	 */
	private static final long serialVersionUID = 5921710666437305274L;

	 /**
     * This level is used to encrypt log message and to avoid writing into DB
     */
    public static final SecureLevel SECURE = new SecureLevel(50000, "SECURE", 0);
    
	protected SecureLevel(int level, String levelStr, int syslogEquivalent) {
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
    public static SecureLevel toLevel(int val, Level defaultLevel)
    {
		return SECURE; 
        
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
    public static SecureLevel toLevel(String sArg, Level defaultLevel)
    {
        return SECURE;
    }
}
